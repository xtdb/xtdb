# Avenue D — built-in vector kernels (#6285 / #4969) — WIP, first numbers

Branch `dart/6285-vector-kernels`. Baseline = xtdb/xtdb main 418e8b4 + harness, `-PnarrowVarTypes`.
Kernel variant = same + this branch, `-PnarrowVarTypes`. SF1, RA Q1/Q6, 50 hot reps/JVM, drop 20, 3 JVMs each, interleaved B K B K B K. Results verified by the bench.

## Round 1 (runs.py --drop 20)
```
base-1.log           q1   cold  11839 n  30 median    8473 mean    8456 min   7954 sd    289
base-1.log           q6   cold   7023 n  30 median    6302 mean    6279 min   5794 sd    257
base-2.log           q1   cold  11720 n  30 median    8636 mean    8667 min   8111 sd    318
base-2.log           q6   cold   6954 n  30 median    6288 mean    6338 min   5660 sd    365
base-3.log           q1   cold  11208 n  30 median    7930 mean    7967 min   7546 sd    249
base-3.log           q6   cold   6526 n  30 median    5762 mean    5805 min   5484 sd    205
kern-1.log           q1   cold  10448 n  30 median    8278 mean    8330 min   7747 sd    351
kern-1.log           q6   cold   5280 n  30 median    5412 mean    5461 min   4956 sd    276
kern-2.log           q1   cold  11251 n  30 median    7731 mean    7757 min   7176 sd    273
kern-2.log           q6   cold   5568 n  30 median    5122 mean    5135 min   4835 sd    150
kern-3.log           q1   cold  10634 n  30 median    7891 mean    7864 min   7290 sd    290
kern-3.log           q6   cold   5219 n  30 median    5215 mean    5236 min   4825 sd    250
```
- Q1: median-of-medians 8346 → 7967 ms (−4.5%)
- Q6: 6117 → 5250 ms (−14.2%)
