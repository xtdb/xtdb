package core2;

public final class BitUtil {
    public static boolean isBitSet(final int mask, final int bit) {
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
}
