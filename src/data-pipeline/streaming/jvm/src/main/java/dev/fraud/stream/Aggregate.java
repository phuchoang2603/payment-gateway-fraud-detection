package dev.fraud.stream;

import static dev.fraud.stream.Records.*;

import java.util.Arrays;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class Aggregate extends KeyedProcessFunction<String, Packet, Output> {
  private final String view, epoch, definition;
  private final boolean logical;
  private final int maxEntries;

  interface MicroClock extends java.io.Serializable {
    long now();
  }

  private final MicroClock clock;
  private transient ValueState<Metadata> metadata;
  private transient MapState<String, Contribution> entries;
  private transient MapState<String, Integer> devices;
  private transient DeadlineQueue deadlines;

  public Aggregate(String view, String epoch, String definition, boolean logical, int maxEntries) {
    this(view, epoch, definition, logical, maxEntries, Wire::wallMicros);
  }

  Aggregate(
      String view,
      String epoch,
      String definition,
      boolean logical,
      int maxEntries,
      MicroClock clock) {
    this.view = view;
    this.epoch = epoch;
    this.definition = definition;
    this.logical = logical;
    this.maxEntries = maxEntries;
    this.clock = clock;
  }

  @Override
  public void open(OpenContext context) {
    metadata =
        getRuntimeContext().getState(new ValueStateDescriptor<>("metadata-jvm-v1", Metadata.class));
    entries =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "contributions-jvm-v1", Types.STRING, Types.POJO(Contribution.class)));
    devices =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("devices-jvm-v1", Types.STRING, Types.INT));
    deadlines = new DeadlineQueue(getRuntimeContext());
  }

  @Override
  public void processElement(Packet packet, Context context, Collector<Output> out)
      throws Exception {
    Metadata state = metadata.value();
    Long previous = deadlines.next();
    if (state == null) {
      state = new Metadata();
      state.integration = packet.fact.integration;
      state.entity = view.equals("customer") ? packet.fact.customer : packet.fact.method;
      state.bootstrap = packet.bootstrap;
      if (!logical)
        for (long window : Wire.WINDOWS)
          if (state.bootstrap + window > packet.now) deadlines.add(state.bootstrap + window, "", 4);
    }
    if (state.bootstrap != packet.bootstrap)
      throw new IllegalStateException("bootstrap boundary changed within checkpoint state");
    if (packet.now >= state.anchor) advance(state, packet.now, true, out);
    else if (logical) throw new IllegalStateException("entity clock regressed");
    String identity = packet.fact.identity();
    Contribution prior = entries.get(identity);
    if (prior != null && !prior.digest.equals(packet.fact.digest))
      throw new IllegalStateException("conflicting attempt start");
    if (prior == null) {
      if (state.size >= maxEntries) throw new IllegalStateException("entity state limit exceeded");
      Contribution contribution = new Contribution();
      contribution.startedAt = packet.fact.startedAt;
      contribution.device = packet.fact.device;
      contribution.digest = packet.fact.digest;
      entries.put(identity, contribution);
      state.size++;
      deadlines.add(contribution.startedAt + 1, identity, 0);
    }
    state.progress = packet.progress;
    state.availableAt = Math.max(state.availableAt, packet.now);
    long now = logical ? packet.now : clock.now();
    advance(state, now, false, out);
    emit(state, now, out);
    metadata.update(state);
    schedule(previous, deadlines.next(), context.timerService());
  }

  private void activate(Metadata state, String identity, Contribution entry, long now)
      throws Exception {
    if (entry.startedAt < now - Wire.DAY) {
      entries.remove(identity);
      state.size--;
      return;
    }
    for (int i = 0; i < Wire.WINDOWS.length; i++) {
      if (entry.startedAt < now - Wire.WINDOWS[i]) continue;
      state.counts[i]++;
      deadlines.add(entry.startedAt + Wire.WINDOWS[i] + 1, identity, i + 1);
      if (i == 2 && view.equals("customer") && entry.device != null) {
        Integer count = devices.get(entry.device);
        if (count == null) {
          count = 0;
          state.distinct++;
        }
        devices.put(entry.device, count + 1);
      }
    }
  }

  private void advance(Metadata state, long now, boolean emitEach, Collector<Output> out)
      throws Exception {
    if (now < state.anchor) throw new IllegalStateException("entity clock regressed");
    Long instant;
    while ((instant = deadlines.next()) != null && instant <= now) {
      long boundary = emitEach ? instant : now;
      String key = deadlines.removeFirst();
      // Expiry at an instant precedes admission at that instant, as in the reference model.
      java.util.ArrayList<Action> activations = new java.util.ArrayList<>();
      while (key != null) {
        Action action = deadlines.take(key);
        key = action.next;
        if (action.kind == 4) continue;
        if (action.kind == 0) {
          activations.add(action);
          continue;
        }
        Contribution entry = entries.get(action.identity);
        if (entry == null) throw new IllegalStateException("expiry has no contribution");
        state.counts[action.kind - 1]--;
        if (action.kind == 3) {
          if (view.equals("customer") && entry.device != null) {
            int count = devices.get(entry.device) - 1;
            if (count == 0) {
              devices.remove(entry.device);
              state.distinct--;
            } else devices.put(entry.device, count);
          }
          entries.remove(action.identity);
          state.size--;
        }
      }
      for (Action action : activations)
        activate(state, action.identity, entries.get(action.identity), boundary);
      if (emitEach) emit(state, boundary, out);
    }
    state.anchor = now;
  }

  private void emit(Metadata state, long now, Collector<Output> out) {
    int coverage = 0;
    for (int i = 0; i < 3; i++) if (now - Wire.WINDOWS[i] >= state.bootstrap) coverage |= 1 << i;
    boolean progressChanged = state.previousConsumed.length != state.progress.length;
    if (!progressChanged) {
      for (int i = 0; i < state.progress.length; i++) {
        if (state.previousConsumed[i] != state.progress[i].consumed) {
          progressChanged = true;
          break;
        }
      }
    }
    if (state.previousCoverage == coverage
        && Arrays.equals(state.previousCounts, state.counts)
        && !progressChanged) return;
    state.previousCoverage = coverage;
    System.arraycopy(state.counts, 0, state.previousCounts, 0, state.counts.length);
    if (state.previousConsumed.length != state.progress.length)
      state.previousConsumed = new long[state.progress.length];
    for (int i = 0; i < state.progress.length; i++)
      state.previousConsumed[i] = state.progress[i].consumed;
    state.revision++;
    state.writeAt = Math.max(now, state.writeAt + 1);
    state.anchor = now;
    out.collect(Wire.snapshot(state, view, epoch, definition, now));
  }

  private void schedule(Long previous, Long next, TimerService timers) {
    Long before = previous == null ? null : Wire.timerAt(previous),
        after = next == null ? null : Wire.timerAt(next);
    if (java.util.Objects.equals(before, after)) return;
    if (before != null) {
      if (logical) timers.deleteEventTimeTimer(before);
      else timers.deleteProcessingTimeTimer(before);
    }
    if (after != null) {
      if (logical) timers.registerEventTimeTimer(after);
      else timers.registerProcessingTimeTimer(after);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext context, Collector<Output> out)
      throws Exception {
    Metadata state = metadata.value();
    if (state == null) return;
    long now = logical ? timestamp * 1000 : clock.now();
    advance(state, now, logical, out);
    if (!logical) emit(state, now, out);
    metadata.update(state);
    // The callback consumed the old registration even if the next deadline rounds to the same
    // millisecond.
    Long next = deadlines.next();
    if (next != null) {
      if (logical) context.timerService().registerEventTimeTimer(Wire.timerAt(next));
      else context.timerService().registerProcessingTimeTimer(Wire.timerAt(next));
    }
  }
}
