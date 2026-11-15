"""Kafka's keyed default partitioner, without a client dependency."""

_SEED = 0x9747B28C
_M = 0x5BD1E995
_R = 24
_MASK = 0xFFFFFFFF


def partition_for(key: str, n_partitions: int) -> int:
    """Return the partition Kafka assigns to ``key``.

    Same function as the producer's keyed partitioner: murmur2 of the UTF-8
    bytes (Kafka ``Utils.murmur2``), then ``hash & 0x7fffffff`` modulo
    ``n_partitions``. Partition ids are ``0 .. n_partitions - 1``.
    """
    if n_partitions < 1:
        msg = "n_partitions must be positive"
        raise ValueError(msg)
    hashed = _murmur2(key.encode("utf-8")) & 0x7FFFFFFF
    return hashed % n_partitions


def _murmur2(data: bytes) -> int:
    """Kafka ``Utils.murmur2``, returned as an unsigned 32-bit value."""
    length = len(data)
    h = (_SEED ^ length) & _MASK
    for block in range(length // 4):
        offset = block * 4
        k = (
            data[offset]
            | (data[offset + 1] << 8)
            | (data[offset + 2] << 16)
            | (data[offset + 3] << 24)
        )
        k = (k * _M) & _MASK
        k ^= k >> _R
        k = (k * _M) & _MASK
        h = (h * _M) & _MASK
        h ^= k
    tail = length & ~3
    remaining = length & 3
    if remaining >= 3:
        h ^= data[tail + 2] << 16
    if remaining >= 2:
        h ^= data[tail + 1] << 8
    if remaining >= 1:
        h ^= data[tail]
        h = (h * _M) & _MASK
    h &= _MASK
    h ^= h >> 13
    h = (h * _M) & _MASK
    h ^= h >> 15
    return h & _MASK
