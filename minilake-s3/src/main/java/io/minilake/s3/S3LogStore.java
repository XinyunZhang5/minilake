package io.minilake.s3;

import io.minilake.storage.LogStore;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * MiniLake's log on S3, with no external lock service.
 *
 * <p>Until 2024 S3 had no atomic create-if-absent, which is why Delta Lake needed DynamoDB to
 * coordinate S3 writers. S3 now supports conditional writes: {@code PutObject} with
 * {@code If-None-Match: *} succeeds only if the key doesn't exist, so it is exactly the
 * {@link LogStore#writeIfAbsent} primitive. A losing writer gets 412 Precondition Failed. If two
 * conditional writes are still racing, S3 may answer 409 Conflict instead, meaning the race isn't
 * decided yet; the request is retried until it either wins or gets the 412.
 *
 * <p>Paths are {@code s3://bucket/key}. Listing relies on S3's strong read-after-write consistency
 * (since December 2020), so a commit is visible to every reader as soon as its PUT returns.
 */
public final class S3LogStore implements LogStore {
  private static final int MAX_CONFLICT_RETRIES = 20;

  private final S3Client s3;

  public S3LogStore(S3Client s3) {
    this.s3 = s3;
  }

  private record Location(String bucket, String key) {
    static Location of(String path) {
      URI uri = URI.create(path);
      if (!"s3".equals(uri.getScheme()) || uri.getHost() == null) {
        throw new IllegalArgumentException("expected s3://bucket/key, got " + path);
      }
      String key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
      return new Location(uri.getHost(), key);
    }
  }

  @Override
  public byte[] read(String path) throws IOException {
    Location l = Location.of(path);
    try {
      return s3.getObjectAsBytes(b -> b.bucket(l.bucket()).key(l.key())).asByteArray();
    } catch (NoSuchKeyException e) {
      throw new NoSuchFileException(path);
    } catch (S3Exception e) {
      throw new IOException("read " + path, e);
    }
  }

  @Override
  public void writeIfAbsent(String path, byte[] data) throws IOException {
    Location l = Location.of(path);
    PutObjectRequest req = PutObjectRequest.builder().bucket(l.bucket()).key(l.key()).ifNoneMatch("*").build();
    for (int attempt = 0; ; attempt++) {
      try {
        s3.putObject(req, RequestBody.fromBytes(data));
        return;
      } catch (S3Exception e) {
        if (e.statusCode() == 412) throw new FileAlreadyExistsException(path);
        if (e.statusCode() != 409 || attempt >= MAX_CONFLICT_RETRIES) throw new IOException("put " + path, e);
        backoff(attempt);
      }
    }
  }

  private static void backoff(int attempt) throws IOException {
    try {
      Thread.sleep(Math.min(200, 5L << Math.min(attempt, 5)) + (long) (Math.random() * 5));
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new java.io.InterruptedIOException();
    }
  }

  @Override
  public void overwrite(String path, byte[] data) throws IOException {
    Location l = Location.of(path);
    try {
      s3.putObject(b -> b.bucket(l.bucket()).key(l.key()), RequestBody.fromBytes(data));
    } catch (S3Exception e) {
      throw new IOException("overwrite " + path, e);
    }
  }

  @Override
  public List<String> list(String dir) throws IOException {
    Location l = Location.of(dir.endsWith("/") ? dir : dir + "/");
    List<String> names = new ArrayList<>();
    try {
      ListObjectsV2Request req = ListObjectsV2Request.builder()
          .bucket(l.bucket()).prefix(l.key()).delimiter("/").build();
      for (S3Object o : s3.listObjectsV2Paginator(req).contents()) {
        names.add(o.key().substring(l.key().length()));
      }
    } catch (S3Exception e) {
      throw new IOException("list " + dir, e);
    }
    names.sort(null); // S3 already lists in UTF-8 binary order; sort to match the LogStore contract exactly
    return names;
  }

  @Override
  public boolean exists(String path) throws IOException {
    Location l = Location.of(path);
    try {
      s3.headObject(b -> b.bucket(l.bucket()).key(l.key()));
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) return false;
      throw new IOException("head " + path, e);
    }
  }
}
