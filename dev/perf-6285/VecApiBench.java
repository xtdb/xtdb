// java --add-modules jdk.incubator.vector dev/perf-6285/VecApiBench.java
// scalar (JIT auto-vectorised) vs jdk.incubator.vector, on the two kernel shapes:
//  - range select over a gathered column, emitting int[] selection
//  - lanewise f64 `p * (1 - d) * (1 + t)`, contiguous and through an int[] gather from off-heap
import jdk.incubator.vector.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Random;

public class VecApiBench {
    static final VectorSpecies<Double> S = DoubleVector.SPECIES_PREFERRED;
    static final int N = 1 << 20;

    static int selScalar(double[] x, double lo, double hi, int[] out) {
        int k = 0;
        for (int i = 0; i < x.length; i++) {
            double v = x[i];
            if (v >= lo && v <= hi) out[k++] = i;
        }
        return k;
    }

    static int selVec(double[] x, double lo, double hi, int[] out) {
        int k = 0, i = 0;
        int ub = S.loopBound(x.length);
        for (; i < ub; i += S.length()) {
            var v = DoubleVector.fromArray(S, x, i);
            var m = v.compare(VectorOperators.GE, lo).and(v.compare(VectorOperators.LE, hi));
            long bits = m.toLong();
            while (bits != 0) {
                int b = Long.numberOfTrailingZeros(bits);
                out[k++] = i + b;
                bits &= bits - 1;
            }
        }
        for (; i < x.length; i++) if (x[i] >= lo && x[i] <= hi) out[k++] = i;
        return k;
    }

    static void projScalar(double[] p, double[] d, double[] t, double[] out) {
        for (int i = 0; i < p.length; i++) out[i] = p[i] * (1 - d[i]) * (1 + t[i]);
    }

    // the shape the kernel actually runs: one pass per op over temporaries
    static void projScalarLanewise(double[] p, double[] d, double[] t) {
        int n = p.length;
        for (int i = 0; i < n; i++) d[i] = 1 - d[i];
        for (int i = 0; i < n; i++) p[i] = p[i] * d[i];
        for (int i = 0; i < n; i++) t[i] = 1 + t[i];
        for (int i = 0; i < n; i++) p[i] = p[i] * t[i];
    }

    static void projVec(double[] p, double[] d, double[] t, double[] out) {
        int i = 0, ub = S.loopBound(p.length);
        for (; i < ub; i += S.length()) {
            var pv = DoubleVector.fromArray(S, p, i);
            var dv = DoubleVector.fromArray(S, d, i);
            var tv = DoubleVector.fromArray(S, t, i);
            pv.mul(dv.neg().add(1)).mul(tv.add(1)).intoArray(out, i);
        }
        for (; i < p.length; i++) out[i] = p[i] * (1 - d[i]) * (1 + t[i]);
    }

    static void gatherScalar(MemorySegment seg, int[] idxs, double[] out) {
        for (int i = 0; i < idxs.length; i++) out[i] = seg.getAtIndex(ValueLayout.JAVA_DOUBLE_UNALIGNED, idxs[i]);
    }

    interface R { void run(); }

    static void time(String name, R r) {
        for (int i = 0; i < 200; i++) r.run();
        long t0 = System.nanoTime();
        int reps = 400;
        for (int i = 0; i < reps; i++) r.run();
        System.out.printf("%-28s %8.3f ns/row%n", name, (System.nanoTime() - t0) / (double) reps / N);
    }

    public static void main(String[] args) {
        System.out.println("species " + S);
        Random rnd = new Random(0);
        double[] p = new double[N], d = new double[N], t = new double[N], out = new double[N];
        for (int i = 0; i < N; i++) { p[i] = rnd.nextDouble() * 1e5; d[i] = rnd.nextInt(11) / 100.0; t[i] = rnd.nextInt(9) / 100.0; }
        int[] sel = new int[N];
        int[] idxs = new int[N];
        for (int i = 0; i < N; i++) idxs[i] = i; // the bitemporal selection is near-identity on a quiescent table
        MemorySegment seg = Arena.global().allocate((long) N * 8, 8);
        for (int i = 0; i < N; i++) seg.setAtIndex(ValueLayout.JAVA_DOUBLE_UNALIGNED, i, d[i]);

        final int[] sink = {0};
        time("sel scalar (Q6 discount)", () -> sink[0] += selScalar(d, 0.05, 0.07, sel));
        time("sel vector-api", () -> sink[0] += selVec(d, 0.05, 0.07, sel));
        time("proj scalar fused", () -> projScalar(p, d, t, out));
        double[] p2 = p.clone(), d2 = d.clone(), t2 = t.clone();
        time("proj scalar lanewise", () -> { System.arraycopy(p, 0, p2, 0, N); System.arraycopy(d, 0, d2, 0, N); System.arraycopy(t, 0, t2, 0, N); projScalarLanewise(p2, d2, t2); });
        time("  (of which 3 arraycopies)", () -> { System.arraycopy(p, 0, p2, 0, N); System.arraycopy(d, 0, d2, 0, N); System.arraycopy(t, 0, t2, 0, N); });
        time("proj vector-api fused", () -> projVec(p, d, t, out));
        time("gather off-heap via int[]", () -> gatherScalar(seg, idxs, out));
        System.out.println(sink[0]);
    }
}
