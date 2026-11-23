#!/usr/bin/env python3
"""Small offline smoke test for the PoC log parser; no Kafka required."""
from analyze import parse

sample = """share-slow | POC EVENT=START MODE=share WORKER=share-slow ID=0000 PARTITION=0 OFFSET=0 AT=2026-11-22T20:00:00.000Z DURATION_MS=0
share-fast | POC EVENT=START MODE=share WORKER=share-fast ID=0001 PARTITION=0 OFFSET=1 AT=2026-11-22T20:00:01.000Z DURATION_MS=0
share-fast | POC EVENT=DONE MODE=share WORKER=share-fast ID=0001 PARTITION=0 OFFSET=1 AT=2026-11-22T20:00:01.100Z DURATION_MS=100
share-slow | POC EVENT=DONE MODE=share WORKER=share-slow ID=0000 PARTITION=0 OFFSET=0 AT=2026-11-22T20:00:20.000Z DURATION_MS=20000"""
rows = list(parse(sample))
assert len(rows) == 4
assert [r[4] for r in rows if r[1] == 'DONE'] == [1, 0]
assert {r[3] for r in rows} == {'share-slow', 'share-fast'}
print('Analyzer smoke test passed')
