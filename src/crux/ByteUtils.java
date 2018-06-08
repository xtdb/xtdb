package crux;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.nio.ByteOrder;

@SuppressWarnings("deprecation")
public class ByteUtils {
    public static final Comparator UNSIGNED_BYTES_COMPARATOR = new UnsignedBytesComparator();

    private static final boolean IS_LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
    private static final sun.misc.Unsafe UNSAFE;

    static {
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int compareBytes(byte[] a, byte[] b, int maxLength) {
        int maxCompareLength = Math.min(Math.min(a.length, b.length), maxLength);
        int maxStrideLength = maxCompareLength & ~(Long.BYTES - 1);
        int i = 0;
        for (; i < maxStrideLength; i += Long.BYTES) {
            int arrayIndex = i + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
            long aLong = UNSAFE.getLong(a, arrayIndex);
            long bLong = UNSAFE.getLong(b, arrayIndex);
            if (aLong != bLong) {
                if (IS_LITTLE_ENDIAN) {
                    return Long.compareUnsigned(Long.reverseBytes(aLong),
                                                Long.reverseBytes(bLong));
                } else {
                    return Long.compareUnsigned(aLong, bLong);
                }
            }
        }
        for (; i < maxCompareLength; i++) {
            int arrayIndex = i + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
            int aByte = UNSAFE.getByte(a, arrayIndex) & 0xff;
            int bByte = UNSAFE.getByte(b, arrayIndex) & 0xff;
            if (aByte != bByte) {
                return aByte - bByte;
            }
        }
        if (i == maxLength) {
            return 0;
        }
        return a.length - b.length;
    }

    public static int compareBytes(byte[] a, byte[] b) {
        return ByteUtils.compareBytes(a, b, Integer.MAX_VALUE);
    }

    public static class UnsignedBytesComparator implements Comparator<byte[]> {
        public int compare(byte[] a, byte[] b) {
            return ByteUtils.compareBytes(a, b, Integer.MAX_VALUE);
        }
    }
}
