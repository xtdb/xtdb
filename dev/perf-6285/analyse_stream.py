# streaming variant of analyse.py (the json one OOMs on 50MB+ recordings)
# usage: analyse_stream.py <jfr> [frame-substring]
import sys, subprocess, collections, re
jfr = sys.argv[1]; want = sys.argv[2] if len(sys.argv) > 2 else None
p = subprocess.Popen(['jfr', 'print', '--stack-depth', '64', '--events', 'jdk.ExecutionSample', jfr], stdout=subprocess.PIPE, text=True, bufsize=1 << 20)
rules = [('page load', ('loadDataPage', 'getRecordBatch', 'MemoryCache')),
         ('predicate: java.time', ('toEpochSecond', 'ZonedDateTime', 'java.time')),
         ('predicate: param-types/zone setup', ('param_types', 'zone_id_cache_buster')),
         ('selection: IntStream/VectorMask/select', ('IntStream', 'VectorMask.open', 'VectorIndirection', 'MaskedRelation.getSelection', 'IntArrayList')),
         ('predicate/projection: expression eval', ('expression$', 'expression.')),
         ('event merge / resolve', ('EntityMerge', 'AsOfResolver', 'EventRowPointer', 'PriorityQueue', 'BitemporalConsumer')),
         ('group-by', ('GroupByCursor', 'group_by$', 'agg.Sum', 'agg.Count', 'GroupMapper', 'DistinctRelationMap')),
         ('project', ('ProjectCursor',))]
n = 0; phase = collections.Counter(); leaf = collections.Counter(); chains = collections.Counter(); other = collections.Counter()
frames = []
def fr(line):
    return re.sub(r'\$\$Lambda[^.]*', '$$Lambda', line.strip().split('(')[0].split(' line:')[0])
def flush():
    global n
    if not frames or not any('ScanCursor.tryAdvance' in f for f in frames): return
    n += 1; leaf[frames[0]] += 1
    hit = None
    for f in frames:
        for name, keys in rules:
            if any(k in f for k in keys): hit = name; break
        if hit: break
    phase[hit or 'other'] += 1
    if not hit: other[' <- '.join(frames[:3])] += 1
    if want:
        i = next((i for i, f in enumerate(frames) if want in f), None)
        if i is not None: chains[' <- '.join(frames[i:i+6])] += 1
in_st = False
for line in p.stdout:
    s = line.strip()
    if s.startswith('jdk.ExecutionSample'):
        flush(); frames = []; in_st = False
    elif s.startswith('stackTrace = ['):
        in_st = True
    elif in_st:
        if s.startswith(']') or s == '...': in_st = False
        elif s: frames.append(fr(s))
flush()
print('samples under ScanCursor.tryAdvance', n)
for m, c in phase.most_common(): print(f"{c:6} {100*c/n:5.1f}% {m}")
print('\nTOP SELF')
for m, c in leaf.most_common(25): print(f"{c:6} {100*c/n:5.1f}% {m}")
print('\nOTHER')
for m, c in other.most_common(8): print(f"{c:5} {m}")
if want:
    print('\nSTACKS THROUGH', want)
    for m, c in chains.most_common(8): print(f"{c:5} {m}\n")
