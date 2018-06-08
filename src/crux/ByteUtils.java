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
        int maxCompareOffset = Math.min(Math.min(a.length, b.length), maxLength) + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
        int maxStrideOffset = maxCompareOffset & ~(Long.BYTES - 1);
        int i = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
        for (; i < maxStrideOffset; i += Long.BYTES) {
            long aLong = UNSAFE.getLong(a, i);
            long bLong = UNSAFE.getLong(b, i);
            if (aLong != bLong) {
                if (IS_LITTLE_ENDIAN) {
                    return Long.compareUnsigned(Long.reverseBytes(aLong),
                                                Long.reverseBytes(bLong));
                } else {
                    return Long.compareUnsigned(aLong, bLong);
                }
            }
        }
        for (; i < maxCompareOffset; i++) {
            byte aByte = UNSAFE.getByte(a, i);
            byte bByte = UNSAFE.getByte(b, i);
            if (aByte != bByte) {
                return Byte.compareUnsigned(aByte, bByte);
            }
        }
        if (i == maxLength + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET) {
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
