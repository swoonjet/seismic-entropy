"""Entropy extraction from raw seismic sample streams.

Each raw sample from a seismometer is a signed integer sensor count. The
high-order bits track real ground motion (structured, predictable moment to
moment); only the least-significant bit flickers unpredictably — sensor
self-noise below the level anything in the extraction pipeline can model.
That's the entropy source. We only ever touch that one bit per sample.

Per station: LSBs accumulate into a bit buffer. Every time 256 bits (32
bytes) are available, they're packed and run through SHA-256. The 256-bit
digest is sliced into eight 32-bit big-endian integers -- the entire digest
used, nothing discarded (unlike a Von Neumann extractor, which thromws away
~75% of its input). SHA-256 also acts as a whitener: even if the LSB stream
carries a slight bias, the digest doesn't inherit it.
"""

import hashlib
import struct
import time


class StationEntropyExtractor:
    """Accumulates LSBs from one station's raw samples and yields 32-bit
    integers as soon as enough bits have arrived for a full hash block."""

    BITS_PER_BLOCK = 256
    BYTES_PER_BLOCK = BITS_PER_BLOCK // 8
    INTS_PER_BLOCK = BITS_PER_BLOCK // 32

    def __init__(self, station_id):
        self.station_id = station_id
        self._bitbuf = 0
        self._bitcount = 0
        self.blocks_hashed = 0

    def feed_samples(self, samples):
        """samples: iterable of raw integer sensor counts. Returns a list of
        (integer, digits_string) tuples -- zero or more, depending on how
        many 256-bit blocks completed during this call."""
        results = []
        for s in samples:
            self._bitbuf = (self._bitbuf << 1) | (int(s) & 1)
            self._bitcount += 1
            if self._bitcount == self.BITS_PER_BLOCK:
                results.extend(self._drain_block())
        return results

    def _drain_block(self):
        block_bytes = self._bitbuf.to_bytes(self.BYTES_PER_BLOCK, "big")
        self._bitbuf = 0
        self._bitcount = 0
        self.blocks_hashed += 1
        digest = hashlib.sha256(block_bytes).digest()
        ints = struct.unpack(">8I", digest)  # 8 x uint32, big-endian
        return [(n, f"{n:010d}") for n in ints]


class EntropyPool:
    """Fans extracted integers out to every registered sink (a WebSocket
    broadcast queue, an API consumption queue, etc.) tagged with station
    and wall-clock time. One StationEntropyExtractor per station id."""

    def __init__(self):
        self._extractors = {}
        self._sinks = []

    def add_sink(self, sink_fn):
        """sink_fn(event: dict) is called for every freshly extracted
        integer, in extraction order."""
        self._sinks.append(sink_fn)

    def ingest(self, station_id, station_label, samples):
        extractor = self._extractors.setdefault(
            station_id, StationEntropyExtractor(station_id)
        )
        for integer, digits in extractor.feed_samples(samples):
            event = {
                "station_id": station_id,
                "station_label": station_label,
                "value": integer,
                "digits": digits,
                "ts": time.time(),
            }
            for sink in self._sinks:
                sink(event)


if __name__ == "__main__":
    # Self-test: feed known LSB patterns and check we hash exactly once
    # every 256 samples, and that consecutive identical blocks (which real
    # entropy should never produce twice) hash to identical output --
    # confirming this is a pure deterministic function of the input bits,
    # not accidentally consuming entropy from anywhere else (e.g. time.time()).
    ext = StationEntropyExtractor("TEST")
    samples = list(range(256))  # LSBs alternate 0,1,0,1,...
    out1 = ext.feed_samples(samples)
    assert len(out1) == 8, f"expected 8 ints from one 256-bit block, got {len(out1)}"
    assert all(0 <= n <= 4294967295 for n, _ in out1)
    assert all(len(d) == 10 for _, d in out1)

    ext2 = StationEntropyExtractor("TEST2")
    out2 = ext2.feed_samples(samples)
    assert out1 == out2, "same input bits must hash to same output"

    # Partial block should not emit anything until it fills.
    ext3 = StationEntropyExtractor("TEST3")
    assert ext3.feed_samples(list(range(255))) == []
    assert len(ext3.feed_samples([1])) == 8  # the 256th sample completes it

    print("entropy.py self-test passed")
    print("example output:", out1[:3])
