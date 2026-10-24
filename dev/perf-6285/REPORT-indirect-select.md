# #6285 avenue A: IndirectRelation applied to select/project (dart, WIP)

First numbers. SF1, Q1/Q6, 50 hot reps, drop 20, all `-PnarrowVarTypes`, interleaved base/spike/sparse/dense x3.
Raw logs in `dev/perf-6285/results/`.

| variant | Q1 medians (3 JVMs) | Q6 medians |
|---|---|---|
| base (main+harness) | 6403 6166 6394 | 4598 4482 4438 |
| spike (31f1699) | 6588 6146 6294 | 4663 4349 4388 |
| sparse (this branch) | 6058 5896 5690 | 4180 3885 4017 |
| dense (this branch, projectMode=dense) | 6222 5977 5882 | 4361 4141 4050 |

Full report to follow.
