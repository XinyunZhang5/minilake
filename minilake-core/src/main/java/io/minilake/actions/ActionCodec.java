package io.minilake.actions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Newline-delimited JSON, one single-key object per action: {"add": {...}}. */
public final class ActionCodec {
  public static final ObjectMapper MAPPER = new ObjectMapper()
      // Omit null fields, but keep null map values: {"part": null} is a real partition value.
      .setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS))
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final Map<String, Class<? extends Action>> BY_KEY = Map.of(
      "protocol", Protocol.class,
      "metaData", Metadata.class,
      "add", AddFile.class,
      "remove", RemoveFile.class,
      "txn", SetTransaction.class,
      "commitInfo", CommitInfo.class);

  private ActionCodec() {}

  static String keyOf(Action a) {
    return switch (a) {
      case Protocol p -> "protocol";
      case Metadata m -> "metaData";
      case AddFile f -> "add";
      case RemoveFile r -> "remove";
      case SetTransaction t -> "txn";
      case CommitInfo c -> "commitInfo";
    };
  }

  public static byte[] encode(Iterable<? extends Action> actions) {
    StringBuilder sb = new StringBuilder();
    try {
      for (Action a : actions) {
        sb.append(MAPPER.writeValueAsString(Map.of(keyOf(a), a))).append('\n');
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  public static List<Action> decode(byte[] bytes) {
    List<Action> out = new ArrayList<>();
    for (String line : new String(bytes, StandardCharsets.UTF_8).split("\n")) {
      if (line.isBlank()) continue;
      try {
        JsonNode node = MAPPER.readTree(line);
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        if (!it.hasNext()) throw new IOException("empty action: " + line);
        Map.Entry<String, JsonNode> e = it.next();
        Class<? extends Action> cls = BY_KEY.get(e.getKey());
        if (cls == null) continue; // forward compatibility: skip unknown actions
        out.add(MAPPER.treeToValue(e.getValue(), cls));
      } catch (IOException e) {
        throw new UncheckedIOException("corrupt log line: " + line, e);
      }
    }
    return out;
  }
}
