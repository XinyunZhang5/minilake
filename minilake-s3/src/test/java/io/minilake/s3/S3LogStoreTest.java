package io.minilake.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.CommitConflictException;
import io.minilake.MiniLakeTable;
import io.minilake.OptimisticTransaction;
import io.minilake.Snapshot;
import io.minilake.actions.AddFile;
import io.minilake.actions.Metadata;
import io.minilake.expr.FileStats;
import io.minilake.schema.DataType;
import io.minilake.schema.Field;
import io.minilake.schema.Schema;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Against LocalStack's S3, which implements conditional PutObject (If-None-Match) like AWS. */
class S3LogStoreTest {
  static GenericContainer<?> localstack;
  static S3Client s3;
  static final String BUCKET = "minilake-test";
  static final Schema SCHEMA = Schema.of(new Field("id", DataType.LONG, false));

  @BeforeAll
  static void start() {
    Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "needs Docker for LocalStack");
    localstack = new GenericContainer<>("localstack/localstack:4.9")
        .withEnv("SERVICES", "s3")
        .withExposedPorts(4566)
        .waitingFor(Wait.forHttp("/_localstack/health").forPort(4566).forStatusCode(200))
        .withStartupTimeout(Duration.ofMinutes(2));
    localstack.start();
    s3 = S3Client.builder()
        .endpointOverride(URI.create("http://" + localstack.getHost() + ":" + localstack.getMappedPort(4566)))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .forcePathStyle(true)
        .build();
    s3.createBucket(b -> b.bucket(BUCKET));
  }

  @AfterAll
  static void stop() {
    if (s3 != null) s3.close();
    if (localstack != null) localstack.stop();
  }

  static String root() { return "s3://" + BUCKET + "/" + UUID.randomUUID(); }

  @Test
  void logStoreContract() throws Exception {
    S3LogStore store = new S3LogStore(s3);
    String dir = root() + "/_log";
    assertEquals(List.of(), store.list(dir));
    store.writeIfAbsent(dir + "/00.json", new byte[] {1});
    assertThrows(FileAlreadyExistsException.class, () -> store.writeIfAbsent(dir + "/00.json", new byte[] {2}));
    assertArrayEquals(new byte[] {1}, store.read(dir + "/00.json"), "the loser did not overwrite");
    assertThrows(NoSuchFileException.class, () -> store.read(dir + "/01.json"));
    store.overwrite(dir + "/00.checkpoint.json", new byte[] {3});
    store.overwrite(dir + "/00.checkpoint.json", new byte[] {4});
    store.writeIfAbsent(dir + "/nested/x", new byte[] {5});
    assertEquals(List.of("00.checkpoint.json", "00.json"), store.list(dir), "direct children only");
    assertTrue(store.exists(dir + "/00.json"));
    assertFalse(store.exists(dir + "/02.json"));
  }

  @Test
  void racingWritersGetExactlyOneWinnerPerKey() throws Exception {
    S3LogStore store = new S3LogStore(s3);
    String dir = root();
    int threads = 8, keys = 30;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    AtomicInteger[] wins = new AtomicInteger[keys];
    for (int k = 0; k < keys; k++) wins[k] = new AtomicInteger();
    CountDownLatch go = new CountDownLatch(1);
    List<Future<?>> fs = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      byte[] me = {(byte) t};
      fs.add(pool.submit(() -> {
        go.await();
        for (int k = 0; k < keys; k++) {
          try {
            store.writeIfAbsent(dir + "/" + k, me);
            wins[k].incrementAndGet();
          } catch (FileAlreadyExistsException lost) {
            // expected for all but one
          }
        }
        return null;
      }));
    }
    go.countDown();
    for (Future<?> f : fs) f.get();
    pool.shutdown();
    for (int k = 0; k < keys; k++) assertEquals(1, wins[k].get(), "key " + k);
  }

  private static AddFile counterFile(long value) {
    String stats = FileStats.toJson(value + 1, Map.of("id", 0L), Map.of("id", value), Map.of("id", 0L));
    return new AddFile(UUID.randomUUID() + ".parquet", Map.of(), 1, 0, true, stats);
  }

  private static long counterValue(AddFile f) {
    return FileStats.parse(f.stats(), SCHEMA).numRecords() - 1;
  }

  /** Same read-modify-write counter as the local and Raft tests, with the log in S3. */
  @Test
  void miniLakeOnS3LosesNoUpdates() throws Exception {
    MiniLakeTable table = MiniLakeTable.forPath(root(), new S3LogStore(s3));
    OptimisticTransaction create = table.startTransaction();
    create.updateMetadata(new Metadata(UUID.randomUUID().toString(), SCHEMA.toJson(), List.of(), Map.of(), 0L));
    create.commit(List.of(counterFile(0)), "CREATE");

    int writers = 4, increments = 20;
    AtomicInteger conflicts = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    List<Future<?>> fs = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      fs.add(pool.submit(() -> {
        for (int ok = 0; ok < increments; ) {
          OptimisticTransaction txn = table.startTransaction();
          AddFile current = txn.scanAll().get(0);
          try {
            txn.commit(List.of(current.remove(0, true), counterFile(counterValue(current) + 1)), "INCREMENT");
            ok++;
          } catch (CommitConflictException e) {
            conflicts.incrementAndGet();
          }
        }
        return null;
      }));
    }
    for (Future<?> f : fs) f.get();
    pool.shutdown();
    Snapshot s = table.latest();
    assertEquals(writers * increments, counterValue(s.allFiles().iterator().next()));
    assertEquals(writers * increments, s.version());
    System.out.printf("s3-backed counter: %d increments, %d conflicts retried%n", writers * increments, conflicts.get());
  }
}
