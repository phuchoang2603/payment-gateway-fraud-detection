package dev.fraud.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

class PackagedJobIT {
  @Test
  @Timeout(240)
  void shadedArtifactRunsInsideFlinkDistribution() throws Exception {
    try (Network network = Network.newNetwork();
        KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092")) {
      kafka
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
      kafka.start();
      String input = "packaged-input",
          clock = "packaged-clock",
          output = "fraud.feature-updates.v2.packaged-test",
          rejects = "fraud.feature-rejects.v1.packaged-test";
      try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
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
      Properties properties = new Properties();
      properties.put("bootstrap.servers", kafka.getBootstrapServers());
      properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      properties.put(
          "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      long base = 1_760_000_000_000_000L;
      var event =
          payments.v1.Events.PaymentEvent.newBuilder()
              .setSchemaVersion(3)
              .setEventId("packaged-event")
              .setIntegrationId("integration")
              .setPaymentId("payment")
              .setAggregateVersion(1)
              .setOccurredAt(Wire.timestamp(base))
              .setRecordedAt(Wire.timestamp(base));
      event
          .getAttemptStartedBuilder()
          .setAttemptId("attempt")
          .getSignalsBuilder()
          .setAttemptedAt(Wire.timestamp(base - 1))
          .getPaymentMethodBuilder()
          .setFingerprint("method");
      event
          .getAttemptStartedBuilder()
          .getContextBuilder()
          .getCustomerBuilder()
          .setCustomerId("customer");
      try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
        producer
            .send(new ProducerRecord<>(clock, KafkaPipelineTest.barrier(0, base - 2 * Wire.DAY, 0)))
            .get();
        producer.send(new ProducerRecord<>(input, Wire.deterministic(event.build()))).get();
        producer
            .send(new ProducerRecord<>(clock, KafkaPipelineTest.barrier(1, base + 2 * Wire.DAY, 1)))
            .get();
      }
      Path root = Path.of(".").toAbsolutePath();
      try (GenericContainer<?> flink =
          new GenericContainer<>(DockerImageName.parse("flink:2.2.1-java17"))) {
        flink
            .withNetwork(network)
            .withEnv("KAFKA_BROKERS", "kafka:19092")
            .withEnv("SOURCE_TOPIC", input)
            .withEnv("SOURCE_PARTITIONS", "1")
            .withEnv("CLOCK_TOPIC", clock)
            .withEnv("CLOCK_MODE", "logical")
            .withEnv("FEATURE_EPOCH", "packaged-test")
            .withEnv("FEATURE_TOPIC", output)
            .withEnv("FEATURE_DEFINITION", "/tmp/definition.yaml")
            .withCopyFileToContainer(
                MountableFile.forHostPath(root.resolve("target/fraud-streaming-1.0.0.jar")),
                "/opt/flink/opt/fraud-streaming.jar")
            .withCopyFileToContainer(
                MountableFile.forHostPath(
                    root.resolve("../../../../contracts/features/fraud-snapshot-v2.yaml")
                        .normalize()),
                "/tmp/definition.yaml")
            .withCommand(
                "bash",
                "-ec",
                "/opt/flink/bin/jobmanager.sh start; /opt/flink/bin/taskmanager.sh start; /opt/flink/bin/flink run -Drestart-strategy.type=none -Dstate.backend.type=rocksdb -c dev.fraud.stream.StreamJob /opt/flink/opt/fraud-streaming.jar || { cat /opt/flink/log/*taskexecutor*.log; exit 1; }")
            .withStartupCheckStrategy(
                new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(150)));
        try {
          flink.start();
        } catch (RuntimeException failure) {
          throw new AssertionError(flink.getLogs(), failure);
        }
        assertEquals(0, flink.getCurrentContainerInfo().getState().getExitCodeLong());
      }
      properties = new Properties();
      properties.put("bootstrap.servers", kafka.getBootstrapServers());
      properties.put("group.id", "packaged-verify");
      properties.put("auto.offset.reset", "earliest");
      properties.put("isolation.level", "read_committed");
      properties.put(
          "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      properties.put(
          "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      int snapshots = 0;
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
        consumer.subscribe(List.of(output));
        long until = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (snapshots < 8 && System.nanoTime() < until)
          for (var record : consumer.poll(Duration.ofMillis(200))) {
            features.v2.Features.SnapshotUpdate.parseFrom(record.value());
            snapshots++;
          }
      }
      assertEquals(8, snapshots);
    }
  }
}
