# streaming variant of analyse.py - `jfr print` text, so it doesn't OOM on a 30MB recording
import sys, collections, re, subprocess

jfr_path = sys.argv[1]
want = sys.argv[2] if len(sys.argv) > 2 else None
p = subprocess.Popen(['jfr', 'print', '--stack-depth', '96', '--events', 'jdk.ExecutionSample', jfr_path],
                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

rules = [('kernel: range select', ('RangePredicate',)),
         ('kernel: f64 projection', ('DoubleProjectionKernel',)),
         ('page load (loadDataPage/getRecordBatch)', ('loadDataPage', 'getRecordBatch', 'MemoryCache')),
         ('predicate: java.time conversion', ('toEpochSecond', 'ZonedDateTime', 'java.time')),
         ('predicate: per-batch param-types/zone setup', ('param_types', 'zone_id_cache_buster')),
         ('predicate: mask->int[] (VectorMask/IntStream)', ('VectorMask', 'IntStream', 'IntPipeline', 'SpinedBuffer')),
         ('predicate/projection: evaluation', ('expression$$reify', 'expression$_GT_expression')),
         ('event merge (EntityMerge/AsOfResolver/PQ)', ('EntityMerge', 'AsOfResolver', 'EventRowPointer', 'PriorityQueue')),
         ('bitemporal consumer build', ('BitemporalConsumer',)),
         ('coalesce copy (concatRels/rowCopier)', ('concatRels', 'RowCopier', 'rowCopier')),
         ('select/indirection compose', ('VectorIndirection', 'IndirectVector.select', 'RelationReader.select')),
         ('project (* price discount)', ('ProjectCursor$$Lambda', 'ProjectCursor.tryAdvance$lambda')),
         ('group-by', ('GroupByCursor', 'group_by$', 'xtdb.operator.group_by', 'agg.'))]

def fr(line):
    line = line.strip()
    line = re.sub(r'\s+line:.*$', '', line)
    line = re.sub(r'\(.*$', '', line)
    return re.sub(r'\$\$Lambda.*', '$$Lambda', line)

phase = collections.Counter(); leaf = collections.Counter(); other = collections.Counter(); chains = collections.Counter()
n = 0; total = 0
frames = None
for line in p.stdout:
    if line.startswith('  stackTrace = ['):
        frames = []
    elif frames is not None:
        if line.startswith('  ]'):
            total += 1
            if any('ScanCursor.tryAdvance' in f for f in frames):
                n += 1
                leaf[frames[0]] += 1
                hit = None
                for f in frames:
                    for name, keys in rules:
                        if any(k in f for k in keys):
                            hit = name; break
                    if hit: break
                hit = hit or 'other'
                phase[hit] += 1
                if hit == 'other': other[' <- '.join(frames[:3])] += 1
                if want and any(want in f for f in frames):
                    i = next(i for i, f in enumerate(frames) if want in f)
                    chains[' <- '.join(frames[i:i+7])] += 1
            frames = None
        elif line.strip() and line.strip() != '...':
            frames.append(fr(line))

print('samples', total, 'under ScanCursor.tryAdvance', n)
for m, c in phase.most_common():
    print(f"{c:6} {100*c/n:5.1f}% {m}")
print('\nTOP SELF')
for m, c in leaf.most_common(15):
    print(f"{c:6} {100*c/n:5.1f}% {m}")
print('\nOTHER')
for m, c in other.most_common(8):
    print(f"{c:5} {m}")
if want:
    print(f'\nSTACKS THROUGH {want}')
    for m, c in chains.most_common(8):
        print(f"{c:5} {m}\n")
