package io.minilake.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import org.junit.jupiter.api.Test;

class FileMapStateMachineTest {
  @Test
  void retriedCommandGetsTheOriginalResult() {
    FileMapStateMachine sm = new FileMapStateMachine();
    Command put = new Command(7, 1, new Op.PutIfAbsent("a", new byte[] {1}));
    assertInstanceOf(Result.Ok.class, sm.apply(put));
    assertInstanceOf(Result.Ok.class, sm.apply(put), "a retry of a winning commit still wins");
    assertInstanceOf(Result.Exists.class, sm.apply(new Command(8, 1, new Op.PutIfAbsent("a", new byte[] {2}))));
  }

  @Test
  void snapshotRestoresFilesAndSessions() {
    FileMapStateMachine sm = new FileMapStateMachine();
    sm.apply(new Command(7, 1, new Op.PutIfAbsent("d/a", new byte[] {1})));
    sm.apply(new Command(7, 2, new Op.Overwrite("d/b", new byte[] {2})));
    sm.apply(new Command(8, 1, new Op.Read("d/a")));
    FileMapStateMachine copy = new FileMapStateMachine();
    copy.restore(sm.snapshot());
    assertEquals(new Result.Names(List.of("a", "b")), copy.apply(new Command(9, 1, new Op.List("d"))));
    assertArrayEquals(new byte[] {1}, ((Result.Value) copy.apply(new Command(9, 2, new Op.Read("d/a")))).data());
    Result replayed = copy.apply(new Command(8, 1, new Op.Read("d/a")));
    assertArrayEquals(new byte[] {1}, ((Result.Value) replayed).data(), "session survived the snapshot");
  }
}
