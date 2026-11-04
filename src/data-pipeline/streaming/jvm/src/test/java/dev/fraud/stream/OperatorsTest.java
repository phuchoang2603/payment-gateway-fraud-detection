package dev.fraud.stream;

import static dev.fraud.stream.Records.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class OperatorsTest {
  static final ObjectMapper JSON = new ObjectMapper();

  static JsonNode fixture() throws Exception {
    return JSON.readTree(Files.readAllBytes(Path.of("target/parity.json")));
  }

  static void noGeneric(org.apache.flink.api.common.ExecutionConfig config) {
    config
        .getSerializerConfig()
        .configure(
            new Configuration().set(PipelineOptions.GENERIC_TYPES, false),
            OperatorsTest.class.getClassLoader());
  }

  static KeyedOneInputStreamOperatorTestHarness<String, Packet, Output> aggregate(
      String view, String definition) throws Exception {
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<String, Packet, Output>(
            new KeyedProcessOperator<>(new Aggregate(view, "jvm-fixture", definition, true, 50000)),
            packet ->
                Wire.compound(
                    packet.fact.integration,
                    view.equals("customer") ? packet.fact.customer : packet.fact.method),
            Types.STRING);
    noGeneric(harness.getExecutionConfig());
    harness.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    return harness;
  }

  static List<String> snapshots(
      KeyedOneInputStreamOperatorTestHarness<String, Packet, Output> harness) {
    List<String> values = new ArrayList<>();
    for (Object item : harness.getOutput())
      if (item instanceof StreamRecord<?> record)
        values.add(Base64.getEncoder().encodeToString(((Output) record.getValue()).value));
    harness.getOutput().clear();
    return values;
  }

  @Test
  void incrementalStateAndCheckpointRestoreMatchPythonByteForByte() throws Exception {
    JsonNode fixture = fixture();
    List<String> actual = new ArrayList<>(), expected = new ArrayList<>();
    fixture.get("expected").forEach(node -> expected.add(node.asText()));
    List<Packet> packets = accepted(fixture);
    for (String view : List.of("customer", "method")) {
      var harness = aggregate(view, fixture.get("definition").asText());
      harness.open();
      try {
        int index = 0;
        for (Packet packet : packets) {
          harness.processElement(new StreamRecord<>(packet, packet.now / 1000));
          harness.processWatermark(new Watermark(packet.now / 1000 - 1));
          if (++index == packets.size() / 2) {
            actual.addAll(snapshots(harness));
            OperatorSubtaskState checkpoint = harness.snapshot(1, 1);
            harness.close();
            harness = aggregate(view, fixture.get("definition").asText());
            harness.initializeState(checkpoint);
            harness.open();
          }
        }
        harness.processWatermark(new Watermark(Long.MAX_VALUE));
        actual.addAll(snapshots(harness));
      } finally {
        harness.close();
      }
    }
    actual.sort(Comparator.naturalOrder());
    expected.sort(Comparator.naturalOrder());
    assertEquals(expected, actual);
  }

  static List<Packet> accepted(JsonNode fixture) throws Exception {
    List<Packet> packets = new ArrayList<>();
    var event =
        new KeyedOneInputStreamOperatorTestHarness<String, Packet, Packet>(
            new KeyedProcessOperator<>(new Deduplicate(true, true)),
            packet -> packet.fact.eventId,
            Types.STRING);
    var attempt =
        new KeyedOneInputStreamOperatorTestHarness<String, Packet, Packet>(
            new KeyedProcessOperator<>(new Deduplicate(true, false)),
            packet -> packet.fact.identity(),
            Types.STRING);
    noGeneric(event.getExecutionConfig());
    noGeneric(attempt.getExecutionConfig());
    event.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    attempt.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    event.open();
    attempt.open();
    try {
      for (JsonNode node : fixture.get("packets")) {
        Packet packet = JSON.treeToValue(node, Packet.class);
        event.processElement(new StreamRecord<>(packet));
        for (Object item : event.getOutput())
          if (item instanceof StreamRecord<?> record)
            attempt.processElement(new StreamRecord<>((Packet) record.getValue()));
        event.getOutput().clear();
        for (Object item : attempt.getOutput())
          if (item instanceof StreamRecord<?> record) packets.add((Packet) record.getValue());
        attempt.getOutput().clear();
      }
      int rejects =
          (event.getSideOutput(StreamJob.REJECTS) == null
                  ? 0
                  : event.getSideOutput(StreamJob.REJECTS).size())
              + (attempt.getSideOutput(StreamJob.REJECTS) == null
                  ? 0
                  : attempt.getSideOutput(StreamJob.REJECTS).size());
      assertEquals(fixture.get("rejects").asInt(), rejects);
      event.processWatermark(new Watermark(Long.MAX_VALUE));
      attempt.processWatermark(new Watermark(Long.MAX_VALUE));
      assertEquals(
          0,
          event
              .getOperator()
              .getKeyedStateBackend()
              .getKeys("identity-jvm-v1", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)
              .count());
      assertEquals(
          0,
          attempt
              .getOperator()
              .getKeyedStateBackend()
              .getKeys("identity-jvm-v1", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)
              .count());
    } finally {
      event.close();
      attempt.close();
    }
    return packets;
  }

  static class TestClock implements Aggregate.MicroClock {
    long instant;

    @Override
    public long now() {
      return instant;
    }
  }

  @Test
  void liveClockIdleTimersCoverageAndInflightPacketsMatchPython() throws Exception {
    JsonNode fixture = fixture();
    TestClock clock = new TestClock();
    long base = 1_760_000_000_000_000L;
    clock.instant = base;
    var operator =
        new Aggregate(
            "customer", "jvm-fixture", fixture.get("definition").asText(), false, 10, clock);
    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<String, Packet, Output>(
            new KeyedProcessOperator<>(operator), packet -> packet.fact.customer, Types.STRING)) {
      noGeneric(harness.getExecutionConfig());
      harness.setStateBackend(new EmbeddedRocksDBStateBackend(true));
      harness.open();
      harness.setProcessingTime(base / 1000);
      Packet packet = new Packet();
      packet.now = base;
      packet.bootstrap = base;
      packet.progress = new Progress[] {new Progress("fixture", 0, 0, 2, base)};
      packet.fact = new Fact();
      packet.fact.eventId = "live-one";
      packet.fact.digest = "one";
      packet.fact.integration = "integration";
      packet.fact.payment = "payment-one";
      packet.fact.attempt = "attempt";
      packet.fact.customer = "customer";
      packet.fact.method = "method";
      packet.fact.device = "device";
      packet.fact.startedAt = base;
      packet.fact.recordedAt = base;
      harness.processElement(new StreamRecord<>(packet));
      clock.instant = base + 1000;
      harness.setProcessingTime(clock.instant / 1000);
      clock.instant = base + 1001;
      packet.fact.eventId = "live-two";
      packet.fact.digest = "two";
      packet.fact.payment = "payment-two";
      packet.fact.startedAt = base - 1;
      packet.progress = new Progress[] {new Progress("fixture", 0, 1, 2, base)};
      harness.processElement(new StreamRecord<>(packet));
      for (long delta : new long[] {5 * Wire.MINUTE + 1000, Wire.HOUR + 1000, Wire.DAY + 1000}) {
        clock.instant = base + delta;
        harness.setProcessingTime(clock.instant / 1000);
      }
      List<String> expected = new ArrayList<>();
      fixture.get("liveExpected").forEach(node -> expected.add(node.asText()));
      assertEquals(expected, snapshots(harness));
    }
  }

  @Test
  void highCardinalityAndHotEntityDrainWithoutWholeEntityState() throws Exception {
    long base = 1_760_000_000_000_000L;
    long began = System.nanoTime();
    try (var harness = aggregate("method", fixture().get("definition").asText())) {
      harness.open();
      for (int index = 0; index < 10000; index++) {
        Packet packet = new Packet();
        packet.now = base + index;
        packet.bootstrap = base - 2 * Wire.DAY;
        packet.progress = new Progress[] {new Progress("load", 0, index, 10000, base)};
        Fact fact = new Fact();
        packet.fact = fact;
        fact.eventId = "event-" + index;
        fact.digest = "digest-" + index;
        fact.integration = "integration";
        fact.payment = "payment-" + index;
        fact.attempt = "attempt";
        fact.customer = "customer";
        fact.method = index % 2 == 0 ? "hot" : "method-" + index % 1000;
        fact.startedAt = base;
        fact.recordedAt = packet.now;
        harness.processElement(new StreamRecord<>(packet));
        if (index % 1000 == 0) harness.getOutput().clear();
      }
      harness.getOutput().clear();
      harness.processWatermark(new Watermark(Long.MAX_VALUE));
      java.util.Map<String, features.v2.Features.SnapshotUpdate> latest = new java.util.HashMap<>();
      for (String encoded : snapshots(harness)) {
        var row =
            features.v2.Features.SnapshotUpdate.parseFrom(Base64.getDecoder().decode(encoded));
        latest.put(row.getEntityId(), row);
      }
      assertEquals(501, latest.size());
      assertTrue(
          latest.values().stream().allMatch(row -> row.getValues().getAttemptCount24H() == 0));
      assertEquals(
          0,
          harness
              .getOperator()
              .getKeyedStateBackend()
              .getKeys(
                  "contributions-jvm-v1", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)
              .count());
      assertEquals(
          0,
          harness
              .getOperator()
              .getKeyedStateBackend()
              .getKeys(
                  "deadline-actions-jvm-v1", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)
              .count());
      System.out.printf(
          "JVM RocksDB load: 10000 records plus expiry drain, %.3f seconds%n",
          (System.nanoTime() - began) / 1e9);
    }
  }

  @Test
  void wireDigestAndTypedSerializersMatchContract() throws Exception {
    JsonNode fixture = fixture();
    byte[] raw = Base64.getDecoder().decode(fixture.get("wire").asText());
    var event = Wire.parse(raw);
    Wire.validate(event);
    assertEquals(fixture.get("digest").asText(), Wire.fact(event).digest);
    assertArrayEquals(raw, Wire.deterministic(event));
    var config = new org.apache.flink.api.common.ExecutionConfig();
    noGeneric(config);
    for (Class<?> type :
        List.of(
            Input.class,
            Fact.class,
            Progress.class,
            Packet.class,
            Output.class,
            Identity.class,
            Contribution.class,
            Action.class,
            Metadata.class,
            ObserveSource.Position.class,
            ObserveSource.Control.class)) {
      assertInstanceOf(org.apache.flink.api.java.typeutils.PojoTypeInfo.class, Types.POJO(type));
      assertDoesNotThrow(() -> Types.POJO(type).createSerializer(config.getSerializerConfig()));
    }
    assertEquals(-1, Wire.micros(Wire.timestamp(-1)));
    assertThrows(Wire.Rejected.class, () -> Wire.parse(new byte[] {(byte) 255}));
    assertThrows(
        Wire.Rejected.class,
        () -> Wire.validate(payments.v1.Events.PaymentEvent.getDefaultInstance()));
  }
}
