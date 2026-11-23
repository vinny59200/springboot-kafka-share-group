#!/usr/bin/env python3
"""Parse docker compose logs; Python 3 standard library only."""
from __future__ import annotations
import collections
import datetime as dt
import pathlib
import re
import sys

PAT = re.compile(r"POC EVENT=(START|DONE) MODE=(\w+) WORKER=([\w-]+) ID=(\d+) PARTITION=(\S+) OFFSET=(\S+) AT=([^\s]+)")

def parse(text: str):
    for line in text.splitlines():
        match = PAT.search(line)
        if match:
            event, mode, worker, ident, part, offset, instant = match.groups()
            yield (dt.datetime.fromisoformat(instant.replace('Z', '+00:00')), event,
                   mode, worker, int(ident), part, offset)

def report(rows):
    if not rows:
        print('No POC START/DONE lines found. Capture with: docker compose logs --no-color share-slow share-fast > share.log')
        return 1
    rows = sorted(rows)
    modes = sorted({r[2] for r in rows})
    for mode in modes:
        subset = [r for r in rows if r[2] == mode]
        starts = [r for r in subset if r[1] == 'START']
        done = sorted((r for r in subset if r[1] == 'DONE'))
        workers = collections.Counter(r[3] for r in done)
        counts = collections.Counter(r[4] for r in done)
        duplicate_ids = {k: v for k, v in counts.items() if v > 1}
        transitions = [(r[0], 1) for r in starts] + [(r[0], -1) for r in done]
        current = peak = 0
        for _, change in sorted(transitions, key=lambda x: (x[0], x[1])):
            current += change
            peak = max(peak, current)
        ids = [r[4] for r in done]
        inversions = sum(a > b for i, a in enumerate(ids) for b in ids[i+1:])
        if starts and done:
            elapsed = (done[-1][0] - starts[0][0]).total_seconds()
        else:
            elapsed = float('nan')
        print(f'\nMODE: {mode}')
        print(f'  START count: {len(starts)} | DONE count: {len(done)}')
        print(f'  Completed by worker: {dict(workers)}')
        print(f'  Unique IDs completed: {len(counts)}')
        print(f'  Duplicated completed IDs: {duplicate_ids or "none observed"}')
        print(f'  Max concurrent handlers: {peak}')
        print(f'  Observed completion-order inversions: {inversions}')
        print(f'  Observed first START -> last DONE: {elapsed:.2f}s')
        print('  First completed IDs:', ids[:20])
        if any(r[5] != '0' for r in subset):
            print('  WARNING: Expected one partition (0). Investigate unexpected partitions.')
    print('\nNotes: results are observations, not formal guarantees; reset between experiments.')
    print('Some in-flight records may be unfinished when logs are captured.')
    return 0

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: python scripts/analyze.py path/to/docker-compose.log')
    data = pathlib.Path(sys.argv[1]).read_bytes()
    if data.startswith((b'\xff\xfe', b'\xfe\xff')):
        content = data.decode('utf-16')
    elif b'\x00' in data[:200]:
        content = data.decode('utf-16-le')
    else:
        content = data.decode('utf-8-sig')
    sys.exit(report(list(parse(content))))
