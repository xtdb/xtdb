package crux;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

@SuppressWarnings("deprecation")
public class ByteUtils {
    public static final Comparator<byte[]> UNSIGNED_BYTES_COMPARATOR = new UnsignedBytesComparator();
    public static final Comparator<DirectBuffer> UNSIGNED_BUFFER_COMPARATOR = new UnsignedBufferComparator();

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

    private static final char[] TWO_BYTES_TO_HEX = new char[256 * 256 * Character.BYTES * 2];

    static {
        for (int i = 0, j = 0; i < 256 * 256; i++, j += 4) {
            String s = String.format("%04x", i);
            TWO_BYTES_TO_HEX[j] = s.charAt(0);
            TWO_BYTES_TO_HEX[j + 1] = s.charAt(1);
            TWO_BYTES_TO_HEX[j + 2] = s.charAt(2);
            TWO_BYTES_TO_HEX[j + 3] = s.charAt(3);
        }
    }

    public static String bytesToHex(final byte[] bytes) {
        return bufferToHex(new UnsafeBuffer(bytes));
    }

    public static String bufferToHex(final DirectBuffer buffer) {
        final int maxOffset = buffer.capacity();
        final long bufferOffset = buffer.addressOffset();
        final byte[] byteArray = buffer.byteArray();
        final char[] acc = new char[maxOffset << 1];
        for (int i = 0,
                 j = sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET;
             i < maxOffset;
             i += Short.BYTES, j += Long.BYTES) {
            if (i == maxOffset - 1) {
                final int b = UNSAFE.getByte(byteArray, i + bufferOffset);
                UNSAFE.putInt(acc, j, UNSAFE.getInt(TWO_BYTES_TO_HEX,
                                                    ((b & 0xFF) << 3)
                                                    + (Character.BYTES << 1)
                                                    + sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET));
            } else {
                short s = UNSAFE.getShort(byteArray, i + bufferOffset);
                s = IS_LITTLE_ENDIAN ? Short.reverseBytes(s) : s;
                UNSAFE.putLong(acc, j, UNSAFE.getLong(TWO_BYTES_TO_HEX,
                                                      ((s & 0xFFFF) << 3)
                                                      + sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET));
            }
        }
        return new String(acc);
    }

    private static final byte[] HEX_TO_NIBBLE = new byte[Byte.MAX_VALUE];

    static {
        for (int i = 0, j = 0; i < 16; i++, j += 4) {
            HEX_TO_NIBBLE[String.format("%x", i).charAt(0)] = (byte) i;
            HEX_TO_NIBBLE[String.format("%X", i).charAt(0)] = (byte) i;
        }
    }

    public static byte[] hexToBytes(final String s) {
        return hexToBuffer(s, new UnsafeBuffer(new byte[s.length() >> 1])).byteArray();
    }

    public static MutableDirectBuffer hexToBuffer(final String s, MutableDirectBuffer buffer) {
        final int len = s.length();
        final byte[] acc = buffer.byteArray();
        final long accOffset = buffer.addressOffset();

        for (int i = 0,
                 j = 0;
             i < len;
             i += 2, j++) {
            UNSAFE.putByte(acc, accOffset + j, (byte) ((HEX_TO_NIBBLE[s.charAt(i)] << 4)
                                                       | HEX_TO_NIBBLE[s.charAt(i + 1)]));
        }
        return buffer;
    }

    public static int compareBytes(final byte[] a, final byte[] b, final int maxLength) {
        return ByteUtils.compareBuffers(new UnsafeBuffer(a), new UnsafeBuffer(b), maxLength);
    }

    public static int compareBytes(final byte[] a, final byte[] b) {
        return ByteUtils.compareBytes(a, b, Integer.MAX_VALUE);
    }

    public static boolean equalBytes(final byte[] a, final byte[] b) {
        return ByteUtils.compareBytes(a, b, Integer.MAX_VALUE) == 0;
    }

    public static boolean equalBytes(final byte[] a, final byte[] b, int maxLength) {
        return ByteUtils.compareBytes(a, b, maxLength) == 0;
    }

    public static class UnsignedBytesComparator implements Comparator<byte[]> {
        public int compare(final byte[] a, final byte[] b) {
            return ByteUtils.compareBytes(a, b, Integer.MAX_VALUE);
        }
    }

    public static int compareBuffers(final DirectBuffer a, final DirectBuffer b, final int maxLength) {
        final int aCapacity = a.capacity();
        final int bCapacity = b.capacity();
        final byte[] aByteArray = a.byteArray();
        final byte[] bByteArray = b.byteArray();
        final long aOffset = a.addressOffset();
        final long bOffset = b.addressOffset();
        final int length = Math.min(Math.min(aCapacity, bCapacity), maxLength);
        final int maxStrideOffset = length & ~(Long.BYTES - 1);

        int i = 0;
        for (; i < maxStrideOffset; i += Long.BYTES) {
            final long aLong = UNSAFE.getLong(aByteArray, aOffset + i);
            final long bLong = UNSAFE.getLong(bByteArray, aOffset + i);
            if (aLong != bLong) {
                if (IS_LITTLE_ENDIAN) {
                    return Long.compareUnsigned(Long.reverseBytes(aLong),
                                                Long.reverseBytes(bLong));
                } else {
                    return Long.compareUnsigned(aLong, bLong);
                }
            }
        }
        for (; i < length; i++) {
            final byte aByte = UNSAFE.getByte(aByteArray, aOffset + i);
            final byte bByte = UNSAFE.getByte(bByteArray, bOffset + i);
            if (aByte != bByte) {
                return (aByte & 0xFF) - (bByte & 0xFF);
            }
        }

        if (i == maxLength) {
            return 0;
        }
        return aCapacity - bCapacity;
    }

    public static int compareBuffers(final DirectBuffer a, final DirectBuffer b) {
        return ByteUtils.compareBuffers(a, b, Integer.MAX_VALUE);
    }

    public static boolean equalBuffers(final DirectBuffer a, final DirectBuffer b) {
        return ByteUtils.compareBuffers(a, b, Integer.MAX_VALUE) == 0;
    }

    public static boolean equalBytes(final DirectBuffer a, final DirectBuffer b, final int maxLength) {
        return ByteUtils.compareBuffers(a, b, maxLength) == 0;
    }

    public static class UnsignedBufferComparator implements Comparator<DirectBuffer> {
        public int compare(final DirectBuffer a, final DirectBuffer b) {
            return ByteUtils.compareBuffers(a, b, Integer.MAX_VALUE);
        }
    }
}
