import json
import pickle
from itertools import combinations

import pytest

from fraud_streaming.replay import OffsetBarrier


def control(sequence, boundary, clock):
    value = {"sequence": sequence, "input_offsets": {"0": boundary}, "clock_us": clock}
    return (json.dumps(value).encode(), "clock", 0, sequence, 0), value


def test_clock_overtaking_and_checkpoint_restore_preserve_input_order():
    inputs = [(b"a", "input", 0, 0, 0), (b"b", "input", 0, 1, 0)]
    clocks = [control(0, 0, 100), control(1, 1, 200), control(2, 2, 300)]
    expected = [clocks[0][0], inputs[0], clocks[1][0], inputs[1], clocks[2][0]]
    for positions in combinations(range(5), 2):
        barrier = OffsetBarrier("input", "clock")
        output, source, controls = [], iter(inputs), iter(clocks)
        for index in range(5):
            row, value = (next(source), None) if index in positions else next(controls)
            output.extend(barrier.accept(row, value))
            barrier = pickle.loads(pickle.dumps(barrier))
        assert output == expected


def test_barrier_refuses_offset_loss_regression_and_unbounded_buffer():
    barrier = OffsetBarrier("input", "clock", max_buffer=1)
    with pytest.raises(ValueError, match="gap"):
        barrier.accept((b"lost", "input", 0, 1, 0))
    barrier.accept((b"a", "input", 0, 0, 0))
    with pytest.raises(ValueError, match="buffer"):
        barrier.accept((b"b", "input", 0, 1, 0))
    barrier = OffsetBarrier("input", "clock")
    with pytest.raises(ValueError, match="bootstrap"):
        barrier.accept(*control(0, 1, 100))
    barrier.accept(*control(0, 0, 100))
    with pytest.raises(ValueError, match="regressed"):
        barrier.accept(*control(1, 0, 99))
