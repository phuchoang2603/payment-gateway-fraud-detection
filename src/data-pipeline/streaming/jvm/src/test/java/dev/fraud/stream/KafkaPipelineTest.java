package dev.fraud.stream;

import static dev.fraud.stream.Records.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class KafkaPipelineTest {
  @Test
  @Timeout(180)
  void boundedReplayUsesNativeKafkaAndCommitsBothTransactionalSinks() throws Exception {
    try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))) {
      kafka
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "900000");
      kafka.start();
      String brokers = kafka.getBootstrapServers(), prefix = "jvm-" + UUID.randomUUID();
      String input = prefix + "-input",
          clock = prefix + "-clock",
          output = prefix + "-output",
          rejects = prefix + "-rejects";
      try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
        admin
            .createTopics(
                List.of(
                    new NewTopic(input, 1, (short) 1),
                    new NewTopic(clock, 1, (short) 1),
                    new NewTopic(output, 1, (short) 1),
                    new NewTopic(rejects, 1, (short) 1)))
            .all()
            .get();
      }
      long base = 1_760_000_000_000_000L;
      byte[] raw =
          java.util.Base64.getDecoder().decode(OperatorsTest.fixture().get("wire").asText());
      var properties = new Properties();
      properties.put("bootstrap.servers", brokers);
      properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      properties.put(
          "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
        producer.send(new ProducerRecord<>(clock, barrier(0, base - 2 * Wire.DAY, 0))).get();
        producer.send(new ProducerRecord<>(input, raw)).get();
        producer.send(new ProducerRecord<>(input, raw)).get();
        producer.send(new ProducerRecord<>(input, new byte[] {(byte) 255})).get();
        producer.send(new ProducerRecord<>(clock, barrier(1, base + 2 * Wire.DAY, 3))).get();
      }
      var config = new Configuration();
      config.set(PipelineOptions.GENERIC_TYPES, false);
      config.setString("state.backend.type", "rocksdb");
      config.setString("taskmanager.memory.process.size", "1024m");
      var env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
      env.enableCheckpointing(1000);
      var source =
          KafkaSource.<Input>builder()
              .setBootstrapServers(brokers)
              .setTopics(input, clock)
              .setGroupId(prefix)
              .setStartingOffsets(OffsetsInitializer.earliest())
              .setBounded(OffsetsInitializer.latest())
              .setDeserializer(new StreamJob.KafkaInput())
              .setProperty("isolation.level", "read_committed")
              .build();
      var observed =
          env.fromSource(source, WatermarkStrategy.noWatermarks(), "input")
              .process(new ObserveSource(brokers, input, 1, true, clock));
      var packets =
          observed.assignTimestampsAndWatermarks(
              WatermarkStrategy.<Packet>forMonotonousTimestamps()
                  .withTimestampAssigner((packet, previous) -> packet.now / 1000));
      var branches =
          StreamJob.updates(
              packets,
              true,
              "jvm-fixture",
              OperatorsTest.fixture().get("definition").asText(),
              50000);
      branches.updates().sinkTo(StreamJob.sink(brokers, output, prefix + "-features-"));
      observed
          .getSideOutput(StreamJob.REJECTS)
          .union(branches.rejects())
          .sinkTo(StreamJob.sink(brokers, rejects, prefix + "-rejects-"));
      env.execute("jvm-replay-kafka-test");
      properties = new Properties();
      properties.put("bootstrap.servers", brokers);
      properties.put("group.id", prefix + "-verify");
      properties.put("auto.offset.reset", "earliest");
      properties.put("isolation.level", "read_committed");
      properties.put(
          "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      properties.put(
          "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      List<features.v2.Features.SnapshotUpdate> snapshots = new ArrayList<>();
      int rejected = 0;
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
        consumer.subscribe(List.of(output, rejects));
        long until = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < until && (snapshots.size() < 8 || rejected < 1)) {
          for (var record : consumer.poll(Duration.ofMillis(200))) {
            if (record.topic().equals(output))
              snapshots.add(features.v2.Features.SnapshotUpdate.parseFrom(record.value()));
            else {
              rejected++;
              assertEquals(
                  "undecodable",
                  new String(
                      record.headers().lastHeader("x-reject-class").value(),
                      StandardCharsets.UTF_8));
            }
          }
        }
      }
      assertEquals(1, rejected);
      assertEquals(8, snapshots.size());
      for (int view : List.of(1, 2)) {
        var rows =
            snapshots.stream()
                .filter(row -> row.getViewValue() == view)
                .sorted(
                    java.util.Comparator.comparingLong(
                        features.v2.Features.SnapshotUpdate::getRevision))
                .toList();
        assertEquals(1, rows.get(0).getValues().getAttemptCount24H());
        assertEquals(0, rows.get(3).getValues().getAttemptCount24H());
      }
    }
  }

  static byte[] barrier(long sequence, long clock, long boundary) {
    return ("{\"sequence\":"
            + sequence
            + ",\"clock_us\":"
            + clock
            + ",\"input_offsets\":{\"0\":"
            + boundary
            + "}}")
        .getBytes(StandardCharsets.UTF_8);
  }
}
