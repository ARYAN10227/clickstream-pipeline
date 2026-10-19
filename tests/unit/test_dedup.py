import random
import re
import uuid

import pytest

from clickpipe.dedup import build_dedup_token

HASH = r"\|h:[0-9a-f]{32}$"


class FakeMessage:
    def __init__(self, partition: int, offset: int) -> None:
        self._partition, self._offset = partition, offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


def _batch(ranges: dict[int, range]) -> list[FakeMessage]:
    return [FakeMessage(p, o) for p, offsets in ranges.items() for o in offsets]


def _ids(n: int, seed: int = 0) -> list[uuid.UUID]:
    rng = random.Random(seed)
    return [uuid.UUID(int=rng.getrandbits(128), version=4) for _ in range(n)]


def _ranges(token: str) -> str:
    """The token without its event-id hash."""
    return re.sub(HASH, "", token)


def test_shuffled_order_gives_same_token() -> None:
    batch, ids = _batch({0: range(100, 200), 3: range(5, 81)}), _ids(176)
    shuffled_batch, shuffled_ids = batch[:], ids[:]
    random.Random(1).shuffle(shuffled_batch)
    random.Random(2).shuffle(shuffled_ids)
    token = build_dedup_token("clicks", batch, ids)
    assert build_dedup_token("clicks", shuffled_batch, shuffled_ids) == token
    assert _ranges(token) == "clicks|p0:100-199|p3:5-80"
    assert re.search(HASH, token)


def test_different_offset_ranges_give_different_tokens() -> None:
    ids = _ids(100)
    first = build_dedup_token("clicks", _batch({0: range(100, 200)}), ids)
    shifted = build_dedup_token("clicks", _batch({0: range(150, 250)}), ids)
    extended = build_dedup_token("clicks", _batch({0: range(100, 201)}), ids)
    other_partition = build_dedup_token("clicks", _batch({1: range(100, 200)}), ids)
    assert len({first, shifted, extended, other_partition}) == 4


def test_same_offsets_with_different_events_give_different_tokens() -> None:
    # e.g. the topic was recreated and offsets start again from 0
    batch = _batch({0: range(0, 500)})
    old = build_dedup_token("clicks", batch, _ids(500, seed=1))
    new = build_dedup_token("clicks", batch, _ids(500, seed=2))
    assert old != new
    assert _ranges(old) == _ranges(new) == "clicks|p0:0-499"


def test_single_message() -> None:
    token = build_dedup_token("clicks", [FakeMessage(2, 42)], _ids(1))
    assert _ranges(token) == "clicks|p2:42-42"


def test_partitions_sorted_by_number_not_text() -> None:
    batch = _batch({10: range(1, 3), 2: range(7, 9), 0: range(0, 1)})
    assert _ranges(build_dedup_token("clicks", batch, _ids(5))) == "clicks|p0:0-0|p2:7-8|p10:1-2"


def test_empty_batch_raises() -> None:
    with pytest.raises(ValueError, match="empty batch"):
        build_dedup_token("clicks", [], [])
