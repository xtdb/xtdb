import json, sys, collections, re, subprocess

jfr_path = sys.argv[1]
raw = subprocess.run(['jfr', 'print', '--json', '--stack-depth', '64', '--events', 'jdk.ExecutionSample', jfr_path],
                     capture_output=True, text=True, check=True).stdout
events = json.loads(raw)['recording']['events']

def fr(f):
    return re.sub(r'\$\$Lambda.*', '$$Lambda', (f['method']['type']['name'] + '.' + f['method']['name']).replace('/', '.'))

tc = collections.Counter(e['values']['sampledThread']['javaName'] for e in events if e['values'].get('stackTrace'))
print('samples', sum(tc.values()), 'threads', tc.most_common(4))

rules = [('page load (loadDataPage/getRecordBatch)', ('loadDataPage', 'getRecordBatch', 'MemoryCache')),
         ('predicate: java.time conversion', ('toEpochSecond', 'ZonedDateTime', 'java.time')),
         ('predicate: per-batch param-types/zone setup', ('param_types', 'zone_id_cache_buster')),
         ('predicate: evaluation', ('expression$$reify',)),
         ('event merge (EntityMerge/AsOfResolver/PQ)', ('EntityMerge', 'AsOfResolver', 'EventRowPointer', 'PriorityQueue')),
         ('project (* price discount)', ('ProjectCursor$$Lambda', 'ProjectCursor.tryAdvance$lambda')),
         ('group-by', ('GroupByCursor$$Lambda', 'group_by$'))]

def in_query(frames):
    return any('ScanCursor.tryAdvance' in f for f in frames)

n = 0
phase = collections.Counter()
other = collections.Counter()
leaf = collections.Counter()
for e in events:
    v = e['values']
    st = v.get('stackTrace')
    if not st:
        continue
    frames = [fr(f) for f in st['frames']]
    if not in_query(frames):
        continue
    n += 1
    leaf[frames[0]] += 1
    hit = None
    for f in frames:
        for name, keys in rules:
            if any(k in f for k in keys):
                hit = name
                break
        if hit:
            break
    hit = hit or 'other'
    phase[hit] += 1
    if hit == 'other':
        other[' <- '.join(frames[:3])] += 1

print('\nsamples under ScanCursor.tryAdvance', n)
for m, c in phase.most_common():
    print(f"{c:5} {100*c/n:5.1f}% {m}")
print('\nTOP SELF')
for m, c in leaf.most_common(15):
    print(f"{c:5} {100*c/n:5.1f}% {m}")
print('\nOTHER')
for m, c in other.most_common(6):
    print(f"{c:4} {m}")

if len(sys.argv) > 2:
    want = sys.argv[2]
    chains = collections.Counter()
    for e in events:
        st = e['values'].get('stackTrace')
        if not st:
            continue
        frames = [fr(f) for f in st['frames']]
        if not in_query(frames) or not any(want in f for f in frames):
            continue
        i = next(i for i, f in enumerate(frames) if want in f)
        chains[' <- '.join(frames[i:i+7])] += 1
    print(f'\nSTACKS THROUGH {want}')
    for m, c in chains.most_common(8):
        print(f"{c:4} {m}\n")
