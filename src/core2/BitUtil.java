package core2;

public final class BitUtil {
    public static boolean isBitSet(int mask, int bit) {
        return (mask & (1 << bit)) != 0;
    }

    public static boolean bitNot(boolean x) {
        return !x;
    }
}
