package dev.fraud.stream;

import static dev.fraud.stream.Records.*;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class Deduplicate extends KeyedProcessFunction<String, Packet, Packet> {
  private final boolean logical, event;
  private transient ValueState<Identity> identities;
  private transient Counter duplicates, tooLate;
  private transient Counter[] lateness;
  private transient java.util.Map<String, Counter> rejected;

  public Deduplicate(boolean logical, boolean event) {
    this.logical = logical;
    this.event = event;
  }

  @Override
  public void open(OpenContext context) {
    identities =
        getRuntimeContext().getState(new ValueStateDescriptor<>("identity-jvm-v1", Identity.class));
    var metrics = getRuntimeContext().getMetricGroup();
    duplicates =
        metrics.addGroup("scope", event ? "event" : "attempt").counter("duplicates_dropped");
    tooLate = metrics.counter("too_late");
    String[] labels = {"lt_1m", "1m_5m", "5m_15m", "15m_24h", "too_late"};
    lateness = new Counter[labels.length];
    for (int i = 0; i < labels.length; i++)
      lateness[i] = metrics.addGroup("lateness", labels[i]).counter("attempts");
    rejected = ObserveSource.rejectCounters(metrics);
  }

  @Override
  public void processElement(Packet packet, Context context, Collector<Packet> out)
      throws Exception {
    Fact fact = packet.fact;
    long late = packet.now - fact.startedAt;
    if (event && late > Wire.DAY) {
      tooLate.inc();
      lateness[4].inc();
      return;
    }
    Identity prior = identities.value();
    if (prior != null && packet.now >= prior.expires) prior = null;
    String content = event ? fact.digest : fact.eventId + ":" + fact.digest;
    if (prior != null) {
      if (prior.content.equals(content)) duplicates.inc();
      else {
        rejected.get("conflicting_duplicate").inc();
        context.output(
            StreamJob.REJECTS,
            Wire.reject(
                new Wire.Rejected(
                    "conflicting_duplicate",
                    "conflicting " + (event ? "event" : "attempt") + " identity/content",
                    fact.eventId),
                packet.source));
      }
      return;
    }
    if (event)
      lateness[
          late < Wire.MINUTE ? 0 : late < 5 * Wire.MINUTE ? 1 : late < 15 * Wire.MINUTE ? 2 : 3]
          .inc();
    long expires = fact.startedAt + Wire.DAY + Wire.HOUR;
    identities.update(new Identity(content, expires));
    if (logical) context.timerService().registerEventTimeTimer(Wire.timerAt(expires));
    else context.timerService().registerProcessingTimeTimer(Wire.timerAt(expires));
    if (!event) packet.source = null;
    out.collect(packet);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext context, Collector<Packet> out)
      throws Exception {
    Identity prior = identities.value();
    if (prior != null && timestamp * 1000 >= prior.expires) identities.clear();
  }
}
