package dev.fraud.stream;

import static dev.fraud.stream.Records.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class StreamJob {
  public static final OutputTag<Output> REJECTS =
      new OutputTag<>("feature-rejects-jvm-v1", Types.POJO(Output.class));

  private StreamJob() {}

  public static class KafkaInput implements KafkaRecordDeserializationSchema<Input> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Input> out) {
      out.collect(
          new Input(
              record.value(),
              record.topic(),
              record.partition(),
              record.offset(),
              record.timestamp()));
    }

    @Override
    public TypeInformation<Input> getProducedType() {
      return Types.POJO(Input.class);
    }
  }

  public static class KafkaOutput implements KafkaRecordSerializationSchema<Output> {
    private final String topic;

    public KafkaOutput(String topic) {
      this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
        Output value, KafkaSinkContext context, Long timestamp) {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, value.key, value.value);
      if (value.headerNames != null)
        for (int i = 0; i < value.headerNames.length; i++)
          record.headers().add(value.headerNames[i], value.headerValues[i]);
      return record;
    }
  }

  public record Branches(DataStream<Output> updates, DataStream<Output> rejects) {}

  public static Branches updates(
      DataStream<Packet> input, boolean logical, String epoch, String definition, int maxEntries) {
    var events =
        input
            .filter(packet -> packet.fact != null)
            .keyBy(packet -> packet.fact.eventId)
            .process(new Deduplicate(logical, true))
            .uid("event-deduplication-jvm-v1")
            .name("Deduplicate events");
    var attempts =
        events
            .keyBy(packet -> packet.fact.identity())
            .process(new Deduplicate(logical, false))
            .uid("attempt-deduplication-jvm-v1")
            .name("Deduplicate attempts");
    var customers =
        attempts
            .keyBy(packet -> Wire.compound(packet.fact.integration, packet.fact.customer))
            .process(new Aggregate("customer", epoch, definition, logical, maxEntries))
            .uid("customer-aggregation-jvm-v1")
            .name("Aggregate customer windows");
    var methods =
        attempts
            .keyBy(packet -> Wire.compound(packet.fact.integration, packet.fact.method))
            .process(new Aggregate("method", epoch, definition, logical, maxEntries))
            .uid("method-aggregation-jvm-v1")
            .name("Aggregate method windows");
    return new Branches(
        customers.union(methods),
        events.getSideOutput(REJECTS).union(attempts.getSideOutput(REJECTS)));
  }

  public static KafkaSink<Output> sink(String brokers, String topic, String transaction) {
    return KafkaSink.<Output>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(new KafkaOutput(topic))
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix(transaction)
        .setProperty("transaction.timeout.ms", "900000")
        .setProperty("compression.type", "zstd")
        .build();
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> config = System.getenv();
    String epoch = required(config, "FEATURE_EPOCH"),
        brokers = required(config, "KAFKA_BROKERS"),
        output = required(config, "FEATURE_TOPIC");
    String source = config.getOrDefault("SOURCE_TOPIC", "payments.events.v1"),
        clock = config.get("CLOCK_TOPIC"),
        run = config.getOrDefault("STREAM_RUN", epoch);
    boolean logical = config.getOrDefault("CLOCK_MODE", "live").equals("logical");
    if (logical
        && (clock == null || epoch.startsWith("live") || source.equals("payments.events.v1")))
      throw new IllegalArgumentException(
          "logical replay requires isolated input, output epoch and clock topic");
    if (!run.equals(epoch) && !(logical && run.startsWith(epoch + "-")))
      throw new IllegalArgumentException("stream run must be its epoch or a history unit of it");
    if (!output.equals("fraud.feature-updates.v2." + run))
      throw new IllegalArgumentException("feature journal must be bound to its epoch");
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig()
        .getSerializerConfig()
        .configure(
            new org.apache.flink.configuration.Configuration()
                .set(org.apache.flink.configuration.PipelineOptions.GENERIC_TYPES, false),
            StreamJob.class.getClassLoader());
    env.setParallelism(1);
    env.enableCheckpointing(10000);
    var builder =
        KafkaSource.<Input>builder()
            .setBootstrapServers(brokers)
            .setGroupId("fraud-features-" + run)
            .setTopics(logical ? java.util.List.of(source, clock) : java.util.List.of(source))
            .setDeserializer(new KafkaInput())
            .setStartingOffsets(
                logical ? OffsetsInitializer.earliest() : OffsetsInitializer.latest())
            .setProperty("isolation.level", "read_committed")
            .setProperty("auto.offset.reset", "none");
    if (logical) builder.setBounded(OffsetsInitializer.latest());
    var observed =
        env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "Kafka payment source")
            .uid("payment-source-jvm-v1")
            .process(
                new ObserveSource(
                    brokers,
                    source,
                    Integer.parseInt(config.getOrDefault("SOURCE_PARTITIONS", "3")),
                    logical,
                    clock))
            .setParallelism(1)
            .setMaxParallelism(1)
            .uid("observed-source-progress-jvm-v1")
            .name("Observe source progress");
    DataStream<Packet> packets =
        logical
            ? observed.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Packet>forMonotonousTimestamps()
                    .withTimestampAssigner((packet, previous) -> packet.now / 1000))
            : observed;
    String definition =
        Wire.hash(
            Files.readAllBytes(
                Path.of(
                    config.getOrDefault(
                        "FEATURE_DEFINITION", "/contracts/features/fraud-snapshot-v2.yaml"))));
    var branches =
        updates(
            packets,
            logical,
            epoch,
            definition,
            Integer.parseInt(config.getOrDefault("MAX_ENTITY_ENTRIES", "50000")));
    branches
        .updates()
        .sinkTo(sink(brokers, output, "fraud-features-" + run + "-"))
        .uid("feature-journal-jvm-v1")
        .name("Feature journal");
    observed
        .getSideOutput(REJECTS)
        .union(branches.rejects())
        .sinkTo(sink(brokers, "fraud.feature-rejects.v1." + run, "fraud-rejects-" + run + "-"))
        .uid("feature-rejects-jvm-v1")
        .name("Feature rejects");
    env.execute("fraud-features-" + run);
  }

  private static String required(Map<String, String> config, String name) {
    String value = config.get(name);
    if (value == null || value.isEmpty()) throw new IllegalArgumentException("missing " + name);
    return value;
  }
}
