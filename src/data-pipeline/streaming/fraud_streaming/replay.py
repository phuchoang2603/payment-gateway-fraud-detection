from collections import deque


class OffsetBarrier:
    def __init__(self, input_topic, clock_topic, max_buffer=20000):
        self.input_topic = input_topic
        self.clock_topic = clock_topic
        self.max_buffer = max_buffer
        self.records = deque()
        self.controls = deque()
        self.received = 0
        self.released = 0
        self.sequence = 0
        self.boundary = 0
        self.clock = None

    def accept(self, row, control=None):
        _, topic, partition, offset, _ = row
        if partition != 0:
            raise ValueError("logical replay requires single-partition topics")
        if topic == self.input_topic:
            if offset != self.received:
                raise ValueError("replay input offset gap or regression")
            self.received += 1
            self.records.append(row)
        elif topic == self.clock_topic:
            if control is None or set(control) != {"clock_us", "input_offsets", "sequence"}:
                raise ValueError("invalid replay barrier")
            boundary = control["input_offsets"]
            clock = control["clock_us"]
            if (
                not isinstance(boundary, dict)
                or boundary.keys() != {"0"}
                or type(boundary["0"]) is not int
                or type(clock) is not int
                or type(control["sequence"]) is not int
                or offset != self.sequence
                or control["sequence"] != self.sequence
                or boundary["0"] < self.boundary
                or (self.clock is not None and clock < self.clock)
                or (self.sequence == 0 and boundary["0"] != 0)
            ):
                raise ValueError("replay barrier regressed or omitted bootstrap")
            self.boundary = boundary["0"]
            self.clock = clock
            self.sequence += 1
            self.controls.append((self.boundary, row))
        else:
            raise ValueError("unexpected replay topic")
        if len(self.records) + len(self.controls) > self.max_buffer:
            raise ValueError("replay barrier buffer exceeded")
        # Inputs wait only for the bootstrap control, and each control waits for the inputs before its
        # boundary. A control that arrives after inputs it had to precede cannot be ordered and fails.
        if self.controls and self.controls[-1][0] < self.released:
            raise ValueError("replay control arrived after inputs past its boundary")
        ready = []
        while True:
            if self.controls and self.controls[0][0] <= self.released:
                ready.append(self.controls.popleft()[1])
            elif (
                self.records
                and self.sequence
                and (not self.controls or self.released < self.controls[0][0])
            ):
                ready.append(self.records.popleft())
                self.released += 1
            else:
                return ready
