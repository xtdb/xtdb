# usage: runs.py [--drop N] <bench-log>...
# per log and query: cold time, then hot median/mean/min over the hot repeats after the first N (default 20)
import json, sys, statistics, re, collections

args = sys.argv[1:]
drop = 20
if args and args[0] == '--drop':
    drop = int(args[1]); args = args[2:]

for path in args:
    cold, hot = {}, collections.defaultdict(list)
    for line in open(path):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        stage = str(r.get('stage', ''))
        m = re.search(r'(q\d+)', stage)
        if not m or 'time-taken-ms' not in r:
            continue
        q = m.group(1)
        if stage.startswith('cold'):
            cold[q] = r['time-taken-ms']
        elif stage.startswith('hot'):
            hot[q].append(r['time-taken-ms'])
    for q in sorted(hot):
        hs = hot[q][drop:]
        if not hs:
            print(f"{path.rsplit('/', 1)[-1]:20} {q:4} only {len(hot[q])} hot repeats"); continue
        print(f"{path.rsplit('/', 1)[-1]:20} {q:4} cold {cold.get(q, '-'):>6} n {len(hs):3} median {statistics.median(hs):7.0f} mean {statistics.mean(hs):7.0f} min {min(hs):6} sd {statistics.pstdev(hs):6.0f}")
