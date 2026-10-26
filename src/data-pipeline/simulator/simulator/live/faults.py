import heapq
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from hashlib import sha256

from confluent_kafka import Consumer, Producer, TopicPartition

from simulator.contracts.payments.v1.events_pb2 import PaymentEvent


def fraction(seed, name, event_id):
    return int.from_bytes(sha256(f"{seed}:{name}:{event_id}".encode()).digest()[:8], "big") / 2**64


def hold_until(settings, due):
    phase = due % settings.hold_interval_seconds
    return due + settings.hold_seconds - phase if phase < settings.hold_seconds else due


def deliveries(settings, seed, event_id, now, with_holds=True):
    delayed = fraction(seed, "late", event_id) < settings.late_rate
    delay = (
        settings.late_min_seconds
        + fraction(seed, "late-delay", event_id) * (settings.late_max_seconds - settings.late_min_seconds)
        if delayed
        else 0
    )
    due = now + delay
    result = [due]
    if fraction(seed, "duplicate", event_id) < settings.duplicate_rate:
        result.append(due + settings.duplicate_delay_seconds)
    return [hold_until(settings, due) for due in result] if with_holds else result


@dataclass
class Pending:
    message: object
    remaining: int
    size: int


def run_faults(config, stop):
    settings = config.faults
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_brokers,
            "group.id": settings.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "fetch.message.max.bytes": settings.max_record_bytes,
            "queued.max.messages.kbytes": max(1, settings.max_bytes // 1024),
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": config.kafka_brokers,
            "enable.idempotence": True,
            "queue.buffering.max.kbytes": max(1, settings.max_bytes // 1024),
        }
    )
    partitions = defaultdict(deque)
    heap, failures = [], []
    count = size = sequence = published = consumed = duplicates = delayed = held = 0
    paused = False
    closing = False
    last_report = time.monotonic()

    def revoke(client, assignment):
        if count and not closing:
            raise RuntimeError("fault assignment revoked with unfinished work; replay required")

    def acknowledge(pending, error, message):
        nonlocal published
        if error:
            failures.append("Kafka publication failed")
        else:
            pending.remaining -= 1
            published += 1

    consumer.subscribe([settings.source], on_revoke=revoke)
    print(
        json.dumps(
            {
                "event": "fault_start",
                "settings": settings.model_dump(),
                "recovery": "at-least-once",
                "boundary": "post-commit-delivery",
            }
        ),
        flush=True,
    )
    try:
        while not stop.is_set():
            if time.monotonic() - last_report >= 5:
                print(
                    json.dumps(
                        {
                            "event": "fault_progress",
                            "consumed": consumed,
                            "acknowledged": published,
                            "pending_records": count,
                            "pending_bytes": size,
                        }
                    ),
                    flush=True,
                )
                last_report = time.monotonic()
            producer.poll(0)
            if failures:
                raise RuntimeError(failures[0])
            now = time.time()
            while heap and heap[0][0] <= now:
                due, number, pending = heap[0]
                message = pending.message
                try:
                    producer.produce(
                        settings.destination,
                        partition=message.partition(),
                        key=message.key(),
                        value=message.value(),
                        headers=message.headers(),
                        timestamp=int(time.time() * 1000),
                        on_delivery=lambda error, result, p=pending: acknowledge(p, error, result),
                    )
                except BufferError:
                    break
                heapq.heappop(heap)
            for partition, queue in partitions.items():
                completed = []
                for pending in queue:
                    if pending.remaining:
                        break
                    completed.append(pending)
                if completed:
                    consumer.commit(
                        offsets=[TopicPartition(settings.source, partition, completed[-1].message.offset() + 1)],
                        asynchronous=False,
                    )
                    for pending in completed:
                        queue.popleft()
                        count -= 1
                        size -= pending.size
            full = count >= settings.max_records or size > settings.max_bytes - settings.max_record_bytes
            assignment = consumer.assignment()
            if assignment and full != paused:
                (consumer.pause if full else consumer.resume)(assignment)
                paused = full
            message = consumer.poll(0.05)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("fault consumer failed")
            raw = message.value()
            event = PaymentEvent.FromString(raw)
            if not event.event_id or not event.payment_id or event.WhichOneof("payload") is None:
                raise ValueError("malformed committed event")
            record_size = (
                len(raw)
                + len(message.key() or b"")
                + sum(len(k.encode()) + len(v or b"") for k, v in message.headers() or [])
            )
            if (
                record_size > settings.max_record_bytes
                or count + 1 > settings.max_records
                or size + record_size > settings.max_bytes
            ):
                raise ValueError("fault record exceeds admission capacity; replay required")
            scheduled = deliveries(settings, config.seed, event.event_id, time.time(), with_holds=False)
            due = [hold_until(settings, when) for when in scheduled]
            pending = Pending(message, len(due), record_size)
            partitions[message.partition()].append(pending)
            for when in due:
                sequence += 1
                heapq.heappush(heap, (when, sequence, pending))
            count += 1
            size += record_size
            consumed += 1
            duplicates += len(due) - 1
            delayed += int(fraction(config.seed, "late", event.event_id) < settings.late_rate)
            held += int(due != scheduled)
    finally:
        producer.flush(10)
        closing = True
        consumer.close()
        print(
            json.dumps(
                {
                    "event": "fault_stop",
                    "consumed": consumed,
                    "acknowledged": published,
                    "duplicates_selected": duplicates,
                    "late_selected": delayed,
                    "burst_held": held,
                    "observed_duplicate_rate": duplicates / consumed if consumed else 0,
                    "observed_late_rate": delayed / consumed if consumed else 0,
                    "observed_held_rate": held / consumed if consumed else 0,
                    "pending_records": count,
                    "pending_bytes": size,
                    "recovery": "uncommitted records replay; additional duplicates possible",
                }
            ),
            flush=True,
        )
