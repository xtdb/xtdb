package crux;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.nio.ByteOrder;

@SuppressWarnings("deprecation")
public class ByteUtils {
    public static Comparator UNSIGNED_BYTES_COMPARATOR = new UnsignedBytesComparator();

    private static sun.misc.Unsafe theUnsafe;
    private static boolean isLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    static {
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int compareBytes(byte[] a, byte[] b, int maxLength) {
        int maxCompareLength = Math.min(Math.min(a.length, b.length), maxLength);
        int maxStrideLength = maxCompareLength - 7;
        int i = 0;
        for (; i < maxStrideLength; i += Long.BYTES) {
            int arrayIndex = i + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
            long aLong = theUnsafe.getLong(a, arrayIndex);
            long bLong = theUnsafe.getLong(b, arrayIndex);
            if (aLong != bLong) {
                if (isLittleEndian) {
                    return Long.compareUnsigned(Long.reverseBytes(aLong),
                                                Long.reverseBytes(bLong));
                } else {
                    return Long.compareUnsigned(aLong, bLong);
                }
            }
        }
        for (; i < maxCompareLength; i++) {
            int arrayIndex = i + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
            int aByte = theUnsafe.getByte(a, arrayIndex) & 0xff;
            int bByte = theUnsafe.getByte(b, arrayIndex) & 0xff;
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
            return ByteUtils.compareBytes(a, b);
        }
    }
}
