import random

import pytest

from clickpipe.dedup import build_dedup_token


class FakeMessage:
    def __init__(self, partition: int, offset: int) -> None:
        self._partition, self._offset = partition, offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


def _batch(ranges: dict[int, range]) -> list[FakeMessage]:
    return [FakeMessage(p, o) for p, offsets in ranges.items() for o in offsets]


def test_shuffled_order_gives_same_token() -> None:
    batch = _batch({0: range(100, 200), 3: range(5, 81)})
    shuffled = batch[:]
    random.Random(1).shuffle(shuffled)
    assert build_dedup_token("clicks", shuffled) == build_dedup_token("clicks", batch)
    assert build_dedup_token("clicks", batch) == "clicks|p0:100-199|p3:5-80"


def test_different_offset_ranges_give_different_tokens() -> None:
    first = build_dedup_token("clicks", _batch({0: range(100, 200)}))
    shifted = build_dedup_token("clicks", _batch({0: range(150, 250)}))
    extended = build_dedup_token("clicks", _batch({0: range(100, 201)}))
    other_partition = build_dedup_token("clicks", _batch({1: range(100, 200)}))
    assert len({first, shifted, extended, other_partition}) == 4


def test_single_message() -> None:
    assert build_dedup_token("clicks", [FakeMessage(2, 42)]) == "clicks|p2:42-42"


def test_partitions_sorted_by_number_not_text() -> None:
    batch = _batch({10: range(1, 3), 2: range(7, 9), 0: range(0, 1)})
    assert build_dedup_token("clicks", batch) == "clicks|p0:0-0|p2:7-8|p10:1-2"


def test_empty_batch_raises() -> None:
    with pytest.raises(ValueError, match="empty batch"):
        build_dedup_token("clicks", [])
