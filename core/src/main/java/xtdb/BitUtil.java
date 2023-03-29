package xtdb;

public final class BitUtil {
    public static boolean isBitSet(final int mask, final int bit) {
        return (mask >>> bit & 1) != 0;
    }

    public static boolean isLongBitSet(final long mask, final int bit) {
        return (mask >>> bit & 1) != 0;
    }

    public static boolean bitNot(final boolean x) {
        return !x;
    }

    public static int unsignedBitShiftRight(final long x, final int n) {
        return (int) (x >>> n);
    }

    public static int bitMask(final long x, final int mask) {
        return ((int) x) & mask;
    }

    public static int rem(final int x, final int y) {
        return x % y;
    }

    public static int log2(final long x) {
        return (Long.SIZE - 1) - Long.numberOfLeadingZeros(x + 1);
    }

    public static long ceilPowerOfTwo(final long x) {
        return 1 << (1 + ((Long.SIZE - 1) - Long.numberOfLeadingZeros(x - 1)));
    }
}
