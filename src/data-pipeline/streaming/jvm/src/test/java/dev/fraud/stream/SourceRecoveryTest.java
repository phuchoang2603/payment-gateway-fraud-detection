package dev.fraud.stream;

import static dev.fraud.stream.Records.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class SourceRecoveryTest {
  static OneInputStreamOperatorTestHarness<Input, Packet> harness(
      String brokers, String topic, String clock) throws Exception {
    var harness =
        new OneInputStreamOperatorTestHarness<Input, Packet>(
            new ProcessOperator<>(new ObserveSource(brokers, topic, 1, true, clock)));
    OperatorsTest.noGeneric(harness.getExecutionConfig());
    harness.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    return harness;
  }

  @Test
  @Timeout(120)
  void checkpointPreservesBufferedInputsAndPendingClockControls() throws Exception {
    try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))) {
      kafka.start();
      String topic = "source-" + UUID.randomUUID(),
          clock = topic + "-clock",
          brokers = kafka.getBootstrapServers();
      try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
      }
      byte[] raw =
          java.util.Base64.getDecoder().decode(OperatorsTest.fixture().get("wire").asText());
      Properties properties = new Properties();
      properties.put("bootstrap.servers", brokers);
      properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      properties.put(
          "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
        producer.send(new ProducerRecord<>(topic, raw)).get();
        producer.send(new ProducerRecord<>(topic, raw)).get();
      }
      long base = 1_760_000_000_000_000L;
      var registry = new org.apache.flink.runtime.state.SharedStateRegistryImpl();
      var harness = harness(brokers, topic, clock);
      harness.open();
      try {
        harness.processElement(new StreamRecord<>(new Input(raw, topic, 0, 0, base / 1000)));
        assertTrue(harness.getOutput().isEmpty());
        OperatorSubtaskState checkpoint = harness.snapshot(1, 1);
        checkpoint.registerSharedStates(registry, 1);
        harness.close();
        harness = harness(brokers, topic, clock);
        harness.initializeState(checkpoint);
        harness.open();
        harness.processElement(
            new StreamRecord<>(
                new Input(
                    KafkaPipelineTest.barrier(0, base - Wire.DAY, 0), clock, 0, 0, base / 1000)));
        var rows = harness.extractOutputValues();
        assertEquals(2, rows.size());
        assertNull(rows.get(0).fact);
        assertNotNull(rows.get(1).fact);
        assertEquals(base - Wire.DAY, rows.get(1).bootstrap);
        assertEquals(0, rows.get(1).progress[0].consumed);
        harness.getOutput().clear();
        harness.processElement(
            new StreamRecord<>(
                new Input(
                    KafkaPipelineTest.barrier(1, base + 2 * Wire.DAY, 2),
                    clock,
                    0,
                    1,
                    base / 1000)));
        assertTrue(harness.getOutput().isEmpty());
        checkpoint = harness.snapshot(2, 2);
        checkpoint.registerSharedStates(registry, 2);
        harness.close();
        harness = harness(brokers, topic, clock);
        harness.initializeState(checkpoint);
        harness.open();
        harness.processElement(new StreamRecord<>(new Input(raw, topic, 0, 1, base / 1000)));
        rows = harness.extractOutputValues();
        assertEquals(2, rows.size());
        assertNotNull(rows.get(0).fact);
        assertNull(rows.get(1).fact);
        assertEquals(base + 2 * Wire.DAY, rows.get(1).now);
        assertEquals(1, rows.get(1).progress[0].consumed);
        var recovered = harness;
        assertThrows(
            IllegalStateException.class,
            () ->
                recovered.processElement(
                    new StreamRecord<>(new Input(raw, topic, 0, 1, base / 1000))));
      } finally {
        harness.close();
      }
    }
  }
}
