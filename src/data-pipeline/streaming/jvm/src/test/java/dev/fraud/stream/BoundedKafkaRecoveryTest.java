package dev.fraud.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BoundedKafkaRecoveryTest {
  @Test
  void restoredBoundedSourceSignalsReadersBeforeAndAfterDiscovery() throws Throwable {
    try (var context = new MockSplitEnumeratorContext<KafkaPartitionSplit>(2);
        var enumerator = enumerator(context, Boundedness.BOUNDED)) {
      enumerator.start();
      context.registerReader(new ReaderInfo(0, "localhost"));
      enumerator.addReader(0);
      context.runNextOneTimeCallable();
      if (!context.getOneTimeCallables().isEmpty()) context.runNextOneTimeCallable();
      context.registerReader(new ReaderInfo(1, "localhost"));
      enumerator.addReader(1);
      assertTrue(context.getSplitsAssignmentSequence().isEmpty());
      assertTrue(context.hasNoMoreSplits(0));
      assertTrue(context.hasNoMoreSplits(1));
    }
  }

  @Test
  void unboundedSourceDoesNotSignalEndOfInput() throws Throwable {
    try (var context = new MockSplitEnumeratorContext<KafkaPartitionSplit>(1);
        var enumerator = enumerator(context, Boundedness.CONTINUOUS_UNBOUNDED)) {
      enumerator.start();
      context.registerReader(new ReaderInfo(0, "localhost"));
      enumerator.addReader(0);
      context.runNextOneTimeCallable();
      assertTrue(context.getOneTimeCallables().isEmpty());
      assertFalse(context.hasNoMoreSplits(0));
    }
  }

  @Test
  void partitionsDiscoveredAfterRestoreAreAssignedBeforeEndOfInput() throws Throwable {
    var events = new ArrayList<String>();
    try (var context =
            new MockSplitEnumeratorContext<KafkaPartitionSplit>(1) {
              @Override
              public void assignSplits(SplitsAssignment<KafkaPartitionSplit> assignment) {
                events.add("assign");
                super.assignSplits(assignment);
              }

              @Override
              public void signalNoMoreSplits(int reader) {
                events.add("end");
                super.signalNoMoreSplits(reader);
              }
            };
        var enumerator = enumerator(context, Boundedness.BOUNDED, true)) {
      enumerator.start();
      context.registerReader(new ReaderInfo(0, "localhost"));
      enumerator.addReader(0);
      assertTrue(events.isEmpty());
      context.runNextOneTimeCallable();
      context.runNextOneTimeCallable();
      assertEquals(List.of("assign", "end"), events);
    }
  }

  private KafkaSourceEnumerator enumerator(
      MockSplitEnumeratorContext<KafkaPartitionSplit> context, Boundedness boundedness) {
    return enumerator(context, boundedness, false);
  }

  private KafkaSourceEnumerator enumerator(
      MockSplitEnumeratorContext<KafkaPartitionSplit> context,
      Boundedness boundedness,
      boolean newPartition) {
    var partition = new TopicPartition("replay", 0);
    var extra = new TopicPartition("replay", 1);
    var properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:1");
    properties.setProperty("group.id", "bounded-recovery-test");
    properties.setProperty("partition.discovery.interval.ms", "-1");
    return new KafkaSourceEnumerator(
        admin -> newPartition ? Set.of(partition, extra) : Set.of(partition),
        OffsetsInitializer.earliest(),
        OffsetsInitializer.offsets(Map.of(partition, 10L, extra, 10L)),
        properties,
        context,
        boundedness,
        new KafkaSourceEnumState(
            List.of(new KafkaPartitionSplit(partition, 10, 10)), List.of(), true));
  }
}
