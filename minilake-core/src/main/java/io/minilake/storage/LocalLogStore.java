package io.minilake.storage;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * POSIX file system store. {@code writeIfAbsent} writes a hidden temp file, then hard-links it to
 * the target: link(2) fails with EEXIST instead of replacing, and the target only ever appears
 * fully written. (A plain CREATE_NEW write would let readers see a half-written commit; a rename
 * would silently overwrite a concurrent winner.)
 */
public final class LocalLogStore implements LogStore {

  @Override
  public byte[] read(String path) throws IOException {
    return Files.readAllBytes(Path.of(path));
  }

  @Override
  public void writeIfAbsent(String path, byte[] data) throws IOException {
    Path target = Path.of(path);
    Files.createDirectories(target.getParent());
    Path tmp = tempSibling(target);
    Files.write(tmp, data);
    try {
      Files.createLink(target, tmp);
    } catch (FileAlreadyExistsException e) {
      throw new FileAlreadyExistsException(path);
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  @Override
  public void overwrite(String path, byte[] data) throws IOException {
    Path target = Path.of(path);
    Files.createDirectories(target.getParent());
    Path tmp = tempSibling(target);
    Files.write(tmp, data);
    try {
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  @Override
  public List<String> list(String dir) throws IOException {
    try (Stream<Path> s = Files.list(Path.of(dir))) {
      return s.map(p -> p.getFileName().toString())
          .filter(n -> !n.startsWith("."))
          .sorted()
          .toList();
    } catch (NoSuchFileException e) {
      return List.of();
    }
  }

  @Override
  public boolean exists(String path) {
    return Files.exists(Path.of(path));
  }

  private static Path tempSibling(Path target) {
    return target.resolveSibling("." + target.getFileName() + "." + UUID.randomUUID() + ".tmp");
  }
}
