package io.minilake;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minilake.actions.Action;
import io.minilake.actions.ActionCodec;
import io.minilake.actions.AddFile;
import io.minilake.actions.CommitInfo;
import io.minilake.actions.Protocol;
import io.minilake.actions.SetTransaction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ActionCodecTest {

  @Test
  void roundTripsEveryActionType() {
    AddFile add = Fixtures.file("a", 1, 10);
    List<Action> actions = List.of(
        Protocol.CURRENT,
        Fixtures.metadata(Map.of("k", "v")),
        add,
        add.remove(42L, true),
        new SetTransaction("app", 7, 99),
        new CommitInfo(123, "WRITE", Map.of("mode", "append"), 4, true));
    assertEquals(actions, ActionCodec.decode(ActionCodec.encode(actions)));
  }

  @Test
  void oneSingleKeyObjectPerLine() {
    String text = new String(ActionCodec.encode(List.of(Protocol.CURRENT)), StandardCharsets.UTF_8);
    assertEquals("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":1}}\n", text);
  }

  @Test
  void skipsUnknownActionsForForwardCompatibility() {
    byte[] log = "{\"futureThing\":{}}\n{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":1}}\n"
        .getBytes(StandardCharsets.UTF_8);
    assertEquals(List.of(Protocol.CURRENT), ActionCodec.decode(log));
  }
}
