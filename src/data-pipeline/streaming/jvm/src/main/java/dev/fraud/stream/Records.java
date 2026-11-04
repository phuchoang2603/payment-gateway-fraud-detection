package dev.fraud.stream;

public final class Records {
  private Records() {}

  public static class Input {
    public byte[] raw;
    public String topic;
    public int partition;
    public long offset;
    public long timestamp;

    public Input() {}

    public Input(byte[] raw, String topic, int partition, long offset, long timestamp) {
      this.raw = raw;
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
      this.timestamp = timestamp;
    }
  }

  public static class Fact {
    public String eventId, digest, integration, payment, attempt, customer, method, device;
    public long startedAt, recordedAt;

    public Fact() {}

    public String identity() {
      return Wire.compound(integration, payment, attempt);
    }
  }

  public static class Progress {
    public String topic;
    public int partition;
    public long consumed, high, observedAt;

    public Progress() {}

    public Progress(String topic, int partition, long consumed, long high, long observedAt) {
      this.topic = topic;
      this.partition = partition;
      this.consumed = consumed;
      this.high = high;
      this.observedAt = observedAt;
    }
  }

  public static class Packet {
    public Fact fact;
    public long now, bootstrap;
    public Progress[] progress;
    public Input source;

    public Packet() {}
  }

  public static class Output {
    public byte[] key, value;
    public String[] headerNames;
    public byte[][] headerValues;

    public Output() {}

    public Output(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class Identity {
    public String content;
    public long expires;

    public Identity() {}

    public Identity(String content, long expires) {
      this.content = content;
      this.expires = expires;
    }
  }

  public static class Contribution {
    public String device, digest;
    public long startedAt;

    public Contribution() {}
  }

  public static class Action {
    public String identity, next;
    public int kind;

    public Action() {}

    public Action(String identity, int kind, String next) {
      this.identity = identity;
      this.kind = kind;
      this.next = next;
    }
  }

  public static class Metadata {
    public String integration, entity;
    public long bootstrap, availableAt, revision, writeAt, anchor;
    public long[] counts = new long[3], previousCounts = new long[3];
    public int size, distinct, previousCoverage = -1;
    public Progress[] progress = new Progress[0];
    public long[] previousConsumed = new long[0];

    public Metadata() {}
  }
}
