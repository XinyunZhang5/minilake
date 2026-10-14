package io.minilake.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * The replicated state: a map from path to bytes (the table's log files), plus one session per
 * client holding its last sequence number and result, so retried commands are answered from the
 * session instead of being applied twice.
 */
public final class FileMapStateMachine {
  private record Session(long seq, Result result) {}

  private final TreeMap<String, byte[]> files = new TreeMap<>();
  private final Map<Long, Session> sessions = new TreeMap<>();
  private final boolean dedup;

  public FileMapStateMachine() { this(true); }

  /** {@code dedup = false} exists only so tests can show the checker catches its absence. */
  FileMapStateMachine(boolean dedup) { this.dedup = dedup; }

  public Result apply(Command c) {
    if (c.op() instanceof Op.Noop) return new Result.Ok();
    if (dedup) {
      Session s = sessions.get(c.clientId());
      if (s != null && c.seq() <= s.seq()) {
        // A client has one request in flight, so an older seq can only be a stale duplicate.
        return c.seq() == s.seq() ? s.result() : new Result.Exists();
      }
    }
    Result r = switch (c.op()) {
      case Op.PutIfAbsent p -> files.putIfAbsent(p.path(), p.data()) == null ? new Result.Ok() : new Result.Exists();
      case Op.Overwrite o -> { files.put(o.path(), o.data()); yield new Result.Ok(); }
      case Op.Read r2 -> new Result.Value(files.get(r2.path()));
      case Op.List l -> new Result.Names(list(l.dir()));
      case Op.Noop n -> new Result.Ok();
    };
    if (dedup) sessions.put(c.clientId(), new Session(c.seq(), r));
    return r;
  }

  private java.util.List<String> list(String dir) {
    String prefix = dir.endsWith("/") ? dir : dir + "/";
    java.util.List<String> out = new ArrayList<>();
    for (String k : files.tailMap(prefix, true).keySet()) {
      if (!k.startsWith(prefix)) break;
      String rest = k.substring(prefix.length());
      if (!rest.contains("/")) out.add(rest);
    }
    return out;
  }

  public byte[] snapshot() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      out.writeInt(files.size());
      for (var e : files.entrySet()) {
        out.writeUTF(e.getKey());
        out.writeInt(e.getValue().length);
        out.write(e.getValue());
      }
      out.writeInt(sessions.size());
      for (var e : sessions.entrySet()) {
        out.writeLong(e.getKey());
        out.writeLong(e.getValue().seq());
        writeResult(out, e.getValue().result());
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void restore(byte[] snapshot) {
    files.clear();
    sessions.clear();
    if (snapshot == null) return;
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot));
      for (int n = in.readInt(); n > 0; n--) {
        String k = in.readUTF();
        files.put(k, in.readNBytes(in.readInt()));
      }
      for (int n = in.readInt(); n > 0; n--) {
        long client = in.readLong();
        sessions.put(client, new Session(in.readLong(), readResult(in)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeResult(DataOutputStream out, Result r) throws IOException {
    switch (r) {
      case Result.Ok ok -> out.writeByte(0);
      case Result.Exists ex -> out.writeByte(1);
      case Result.Value v -> {
        out.writeByte(2);
        out.writeInt(v.data() == null ? -1 : v.data().length);
        if (v.data() != null) out.write(v.data());
      }
      case Result.Names n -> {
        out.writeByte(3);
        out.writeInt(n.names().size());
        for (String s : n.names()) out.writeUTF(s);
      }
    }
  }

  private static Result readResult(DataInputStream in) throws IOException {
    return switch (in.readByte()) {
      case 0 -> new Result.Ok();
      case 1 -> new Result.Exists();
      case 2 -> {
        int len = in.readInt();
        yield new Result.Value(len < 0 ? null : in.readNBytes(len));
      }
      case 3 -> {
        java.util.List<String> names = new ArrayList<>();
        for (int n = in.readInt(); n > 0; n--) names.add(in.readUTF());
        yield new Result.Names(names);
      }
      default -> throw new IOException("bad result tag");
    };
  }
}
