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

    private static final char[] HEX = new char[256 * 256 * 4];

    static {
        for (int i = 0, j = 0; i < 256 * 256; i++, j += 4) {
            String s = String.format("%04x", i);
            HEX[j] = s.charAt(0);
            HEX[j + 1] = s.charAt(1);
            HEX[j + 2] = s.charAt(2);
            HEX[j + 3] = s.charAt(3);
        }
    }

    public static String bytesToHex(byte[] bytes) {
        char[] acc = new char[bytes.length << 1];
        int maxOffset = bytes.length + sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
        for (int i = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET, j = sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET;
             i < maxOffset;
             i += 2, j += 8) {
            if (i == maxOffset - 1) {
                int b = UNSAFE.getByte(bytes, i);
                UNSAFE.putInt(acc, j, UNSAFE.getInt(HEX,
                                                    ((b & 0xFF) << 3)
                                                    + (Character.BYTES << 1)
                                                    + sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET));
            } else {
                short s = UNSAFE.getShort(bytes, i);
                s = IS_LITTLE_ENDIAN ? Short.reverseBytes(s) : s;
                UNSAFE.putLong(acc, j, UNSAFE.getLong(HEX,
                                                      ((s & 0xFFFF) << 3)
                                                      + sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET));
            }
        }
        return new String(acc);
    }

    public static byte[] hexToBytes(String s) {
        int len = s.length();
        byte[] acc = new byte[len >> 1];
        for (int i = 0, j = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
             i < len;
             i += 2, j++) {
            UNSAFE.putByte(acc, j, (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                           | Character.digit(s.charAt(i + 1), 16)));
        }
        return acc;
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
                return (aByte & 0xFF) - (bByte & 0xFF);
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
