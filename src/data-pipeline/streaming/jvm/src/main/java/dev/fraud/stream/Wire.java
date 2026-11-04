package dev.fraud.stream;

import static dev.fraud.stream.Records.*;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Timestamp;
import features.v2.Features;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import payments.v1.Events;

public final class Wire {
  public static final long SECOND = 1_000_000L,
      MINUTE = 60 * SECOND,
      HOUR = 60 * MINUTE,
      DAY = 24 * HOUR;
  public static final long[] WINDOWS = {5 * MINUTE, HOUR, DAY};
  private static final ThreadLocal<MessageDigest> SHA =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException(e);
            }
          });

  private Wire() {}

  public static class Rejected extends RuntimeException {
    public final String kind, eventId;

    public Rejected(String kind, String message, String eventId) {
      super(message);
      this.kind = kind;
      this.eventId = eventId;
    }
  }

  public static byte[] deterministic(MessageLite message) {
    byte[] bytes = new byte[message.getSerializedSize()];
    CodedOutputStream out = CodedOutputStream.newInstance(bytes);
    out.useDeterministicSerialization();
    try {
      message.writeTo(out);
      out.checkNoSpaceLeft();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return bytes;
  }

  public static String hash(byte[] bytes) {
    return HexFormat.of().formatHex(SHA.get().digest(bytes));
  }

  public static String compound(String... parts) {
    StringBuilder key = new StringBuilder();
    for (String part : parts)
      key.append(part.getBytes(StandardCharsets.UTF_8).length).append(':').append(part);
    return key.toString();
  }

  public static byte[] entityKey(String epoch, String view, String integration, String entity) {
    return hash(compound(epoch, view, integration, entity).getBytes(StandardCharsets.UTF_8))
        .getBytes(StandardCharsets.US_ASCII);
  }

  public static long micros(Timestamp stamp) {
    if (stamp.getSeconds() < -62135596800L
        || stamp.getSeconds() > 253402300799L
        || stamp.getNanos() < 0
        || stamp.getNanos() > 999999999) throw new IllegalArgumentException("invalid timestamp");
    return stamp.getSeconds() * SECOND + stamp.getNanos() / 1000;
  }

  public static Timestamp timestamp(long micros) {
    return Timestamp.newBuilder()
        .setSeconds(Math.floorDiv(micros, SECOND))
        .setNanos((int) Math.floorMod(micros, SECOND) * 1000)
        .build();
  }

  public static long wallMicros() {
    java.time.Instant now = java.time.Instant.now();
    return now.getEpochSecond() * SECOND + now.getNano() / 1000;
  }

  public static long timerAt(long micros) {
    return Math.floorDiv(micros, 1000) + (Math.floorMod(micros, 1000) == 0 ? 0 : 1);
  }

  public static Events.PaymentEvent parse(byte[] raw) {
    try {
      return Events.PaymentEvent.parseFrom(raw);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new Rejected("undecodable", "payment event is not decodable protobuf", null);
    }
  }

  public static void validate(Events.PaymentEvent event) {
    if (event.getSchemaVersion() != 3
        || event.getEventId().isEmpty()
        || event.getIntegrationId().isEmpty()
        || event.getPaymentId().isEmpty()
        || event.getAggregateVersion() == 0
        || !event.hasOccurredAt()
        || !event.hasRecordedAt()
        || event.getPayloadCase() == Events.PaymentEvent.PayloadCase.PAYLOAD_NOT_SET)
      throw new Rejected(
          "invalid_event", "invalid or unsupported payment event", event.getEventId());
    try {
      micros(event.getOccurredAt());
      micros(event.getRecordedAt());
    } catch (IllegalArgumentException e) {
      throw new Rejected("invalid_event", "invalid payment event timestamp", event.getEventId());
    }
  }

  public static Fact fact(Events.PaymentEvent event) {
    if (!event.hasAttemptStarted()) return null;
    Events.AttemptStarted body = event.getAttemptStarted();
    if (body.getAttemptId().isEmpty()
        || body.getContext().getCustomer().getCustomerId().isEmpty()
        || body.getSignals().getPaymentMethod().getFingerprint().isEmpty()
        || !body.getSignals().hasAttemptedAt()
        || (body.getSignals().hasDeviceFingerprint()
            && body.getSignals().getDeviceFingerprint().isEmpty()))
      throw new Rejected(
          "invalid_context", "invalid self-contained attempt context", event.getEventId());
    Fact fact = new Fact();
    try {
      fact.startedAt = micros(body.getSignals().getAttemptedAt());
    } catch (IllegalArgumentException e) {
      throw new Rejected("invalid_context", "invalid attempt time", event.getEventId());
    }
    fact.eventId = event.getEventId();
    fact.digest = hash(deterministic(event));
    fact.integration = event.getIntegrationId();
    fact.payment = event.getPaymentId();
    fact.attempt = body.getAttemptId();
    fact.recordedAt = micros(event.getRecordedAt());
    fact.customer = body.getContext().getCustomer().getCustomerId();
    fact.method = body.getSignals().getPaymentMethod().getFingerprint();
    fact.device =
        body.getSignals().hasDeviceFingerprint() ? body.getSignals().getDeviceFingerprint() : null;
    return fact;
  }

  public static Output reject(Rejected error, Input source) {
    Output result =
        new Output(
            error.eventId == null || error.eventId.isEmpty()
                ? null
                : error.eventId.getBytes(StandardCharsets.UTF_8),
            source.raw);
    result.headerNames =
        new String[] {
          "x-reject-class",
          "x-reject-message",
          "x-source-topic",
          "x-source-partition",
          "x-source-offset",
          "x-source-timestamp"
        };
    String message = error.getMessage();
    result.headerValues =
        new byte[][] {
          error.kind.getBytes(StandardCharsets.UTF_8),
          message.substring(0, Math.min(message.length(), 200)).getBytes(StandardCharsets.US_ASCII),
          source.topic.getBytes(StandardCharsets.UTF_8),
          Integer.toString(source.partition).getBytes(StandardCharsets.US_ASCII),
          Long.toString(source.offset).getBytes(StandardCharsets.US_ASCII),
          Long.toString(source.timestamp).getBytes(StandardCharsets.US_ASCII)
        };
    return result;
  }

  public static Output snapshot(
      Metadata state, String view, String epoch, String definition, long now) {
    Features.SnapshotValues.Builder values = Features.SnapshotValues.newBuilder();
    if (now - WINDOWS[0] >= state.bootstrap) values.setAttemptCount5M(state.counts[0]);
    if (now - WINDOWS[1] >= state.bootstrap) values.setAttemptCount1H(state.counts[1]);
    if (now - WINDOWS[2] >= state.bootstrap) {
      values.setAttemptCount24H(state.counts[2]);
      if (view.equals("customer")) values.setDistinctDevices24H(state.distinct);
    }
    Features.WindowCoverage.Builder coverage = Features.WindowCoverage.newBuilder();
    coverage
        .setFiveMinutesValue(now - WINDOWS[0] >= state.bootstrap ? 2 : 1)
        .setOneHourValue(now - WINDOWS[1] >= state.bootstrap ? 2 : 1)
        .setTwentyFourHoursValue(now - WINDOWS[2] >= state.bootstrap ? 2 : 1);
    Features.SnapshotUpdate.Builder update =
        Features.SnapshotUpdate.newBuilder()
            .setSchemaVersion(1)
            .setDefinition(
                Features.DefinitionIdentity.newBuilder()
                    .setVersion("fraud-snapshot-v2")
                    .setSha256(definition))
            .setEpoch(epoch)
            .setViewValue(view.equals("customer") ? 1 : 2)
            .setIntegrationId(state.integration)
            .setEntityId(state.entity)
            .setRevision(state.revision)
            .setAnchor(timestamp(now))
            .setSourceAvailableAt(timestamp(state.availableAt))
            .setOnlineWriteAt(timestamp(state.writeAt))
            .setCoverage(coverage)
            .setValues(values);
    for (Progress progress : state.progress)
      update.addSourceProgress(
          Features.SourcePartitionProgress.newBuilder()
              .setTopic(progress.topic)
              .setPartition(progress.partition)
              .setConsumedOffset(progress.consumed)
              .setObservedHighWatermark(progress.high)
              .setObservedAt(timestamp(progress.observedAt)));
    update.setSnapshotId(hash(deterministic(update.build())));
    return new Output(
        entityKey(epoch, view, state.integration, state.entity), deterministic(update.build()));
  }
}
