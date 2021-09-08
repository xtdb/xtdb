package xtdb.morton;

import java.math.BigInteger;
import java.util.Arrays;
import clojure.lang.BigInt;

public class UInt128 extends Number implements Comparable<UInt128> {
    private static final long serialVersionUID = 4323374720764034739L;

    private static BigInteger unsignedLongToBigInteger(long x) {
        BigInteger b = BigInteger.valueOf(x & Long.MAX_VALUE);
        if (x < 0) {
            return b.setBit(Long.SIZE - 1);
        }
        return b;
    }

    public static UInt128 fromBigInteger(BigInteger x) {
        return new UInt128(x.shiftRight(Long.SIZE).longValue(), x.longValue());
    }

    public static UInt128 fromLong(long x) {
        return new UInt128(0, x);
    }

    public static UInt128 fromNumber(Number x) {
        if (x instanceof UInt128) {
            return (UInt128) x;
        }
        if (x instanceof BigInteger) {
            return UInt128.fromBigInteger((BigInteger) x);
        }
        if (x instanceof BigInt) {
            return UInt128.fromBigInteger(((BigInt) x).toBigInteger());
        }
        return UInt128.fromLong(x.longValue());
    }

    public static final int BYTES = 16;
    public static final int SIZE = 128;

    public static final UInt128 ZERO = new UInt128(0, 0);
    public static final UInt128 ONE = new UInt128(0, 1);
    public static final UInt128 MAX = new UInt128(-1, -1);

    public final long upper, lower;

    public UInt128(long upper, long lower) {
        this.upper = upper;
        this.lower = lower;
    }

    public UInt128 and(UInt128 other) {
        return new UInt128(upper & other.upper, lower & other.lower);
    }

    public UInt128 or(UInt128 other) {
        return new UInt128(upper | other.upper, lower | other.lower);
    }

    public UInt128 xor(UInt128 other) {
        return new UInt128(upper ^ other.upper, lower ^ other.lower);
    }

    public UInt128 not() {
        return new UInt128(~upper, ~lower);
    }

    public UInt128 shiftLeft(int n) {
        if (n == 0) {
            return this;
        }
        long lower = this.lower;
        long upper = this.upper;
        if (n >= Long.SIZE) {
            int shift = n - Long.SIZE;
            if (shift >= Long.SIZE) {
                return new UInt128(0, 0);
            }
            return new UInt128(lower << shift, 0);
        }
        return new UInt128(upper << n | lower >>> (Long.SIZE - n), lower << n);
    }

    public UInt128 shiftRight(int n) {
        if (n == 0) {
            return this;
        }
        long lower = this.lower;
        long upper = this.upper;
        if (n >= Long.SIZE) {
            int shift = n - Long.SIZE;
            if (shift >= Long.SIZE) {
                return new UInt128(0, 0);
            }
            return new UInt128(0, upper >>> shift);
        }
        return new UInt128(upper >>> n, upper << (Long.SIZE - n) | lower >>> n);
    }

    public UInt128 dec() {
        if (lower == 0) {
            return new UInt128(upper - 1, -1);
        }
        return new UInt128(upper, lower - 1);
    }

    public boolean testBit(int n) {
        if (n >= Long.SIZE) {
            return ((upper >>> (n - Long.SIZE)) & 1) == 1;
        } else {
            return ((lower >>> n) & 1) == 1;
        }
    }

    public int numberOfLeadingZeros() {
        if (upper != 0) {
            return Long.numberOfLeadingZeros(upper);
        } else {
            return Long.numberOfLeadingZeros(lower) + Long.SIZE;
        }
    }

    public double doubleValue() {
        return (double) lower;
    }

    public float floatValue() {
        return (float) lower;
    }

    public int intValue() {
        return (int) lower;
    }

    public long longValue() {
        return lower;
    }

    public BigInteger bigIntegerValue() {
        return unsignedLongToBigInteger(upper).shiftLeft(Long.SIZE).or(unsignedLongToBigInteger(lower));
    }

    public String toString() {
        return toString(10);
    }

    public String toString(int radix) {
        return bigIntegerValue().toString(radix);
    }

    public boolean equals(Object that) {
        if (that instanceof UInt128) {
            UInt128 other = (UInt128) that;
            return this.upper == other.upper && this.lower == other.lower;
        }
        if (that instanceof Number && upper == 0) {
            Number other = (Number) that;
            return this.longValue() == other.longValue();
        }
        if (that instanceof BigInteger) {
            return this.bigIntegerValue().equals(that);
        }
        return false;
    }

    public int hashCode() {
        return Arrays.hashCode(new long[] {upper, lower});
    }

    // NOTE: Using clojure.core/compare isn't safe, as it will use
    // longValue for any Number implementation it doesn't know about.
    public int compareTo(UInt128 that) {
        int diff = Long.compareUnsigned(upper, that.upper);
        if (diff == 0) {
            return Long.compareUnsigned(lower, that.lower);
        }
        return diff;
    }
}
