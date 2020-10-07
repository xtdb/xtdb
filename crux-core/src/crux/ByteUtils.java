package crux;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ByteUtils {
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
                UNSAFE.putInt(acc, (long)j, UNSAFE.getInt(TWO_BYTES_TO_HEX,
                                                          (long)(((b & 0xFF) << 3)
                                                                 + (Character.BYTES << 1)
                                                                 + sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET)));
            } else {
                short s = UNSAFE.getShort(byteArray, i + bufferOffset);
                s = IS_LITTLE_ENDIAN ? Short.reverseBytes(s) : s;
                UNSAFE.putLong(acc, (long)j, UNSAFE.getLong(TWO_BYTES_TO_HEX,
                                                            (long)(((s & 0xFFFF) << 3)
                                                                   + sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET)));
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
        return new UnsafeBuffer(buffer, 0, len >> 1);
    }

    public static int compareBuffers(final DirectBuffer a, final DirectBuffer b, final int maxLength) {
        if (a.byteArray() != null || b.byteArray() != null) {
            return compareBuffersSlowPath(a, b, maxLength);
        }
        final int aCapacity = a.capacity();
        final int bCapacity = b.capacity();
        final long aOffset = a.addressOffset();
        final long bOffset = b.addressOffset();
        final int length = Math.min(Math.min(aCapacity, bCapacity), maxLength);
        final int maxStrideOffset = length & ~(Long.BYTES - 1);

        int i = 0;
        for (; i < maxStrideOffset; i += Long.BYTES) {
            final long aLong = UNSAFE.getLong(aOffset + i);
            final long bLong = UNSAFE.getLong(bOffset + i);
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
            final byte aByte = UNSAFE.getByte(aOffset + i);
            final byte bByte = UNSAFE.getByte(bOffset + i);
            if (aByte != bByte) {
                return (aByte & 0xff) - (bByte & 0xff);
            }
        }

        if (i == maxLength) {
            return 0;
        }
        return aCapacity - bCapacity;
    }

    private static int compareBuffersSlowPath(final DirectBuffer a, final DirectBuffer b, final int maxLength) {
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
            final long bLong = UNSAFE.getLong(bByteArray, bOffset + i);
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
                return (aByte & 0xff) - (bByte & 0xff);
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

    public static boolean equalBuffers(final DirectBuffer a, final DirectBuffer b, final int maxLength) {
        return ByteUtils.compareBuffers(a, b, maxLength) == 0;
    }

    public static class UnsignedBufferComparator implements Comparator<DirectBuffer> {
        public int compare(final DirectBuffer a, final DirectBuffer b) {
            return ByteUtils.compareBuffers(a, b, Integer.MAX_VALUE);
        }
    }

    public static int binarySearchBuffer(final DirectBuffer a, final int key) {
        return binarySearchBuffer(a, key, ByteOrder.BIG_ENDIAN);
    }

    public static int binarySearchBuffer(final DirectBuffer a, final int key, final ByteOrder order) {
        int low = 0;
        int high = (a.capacity() >>> 2) - 1;
        while (low <= high) {
            final int idx = (low + high) >> 1;
            final int element = a.getInt(idx << 2, order);
            if (element == key) {
                return idx;
            } else if (element > key) {
                high = idx - 1;
            } else {
                low = idx + 1;
            }
        }
        return -1;
    }

    // https://en.wikipedia.org/wiki/SHA-1#SHA-1_pseudocode

    private static final int SHA1_BLOCK_BYTES = 512 / Byte.SIZE;
    private static final int SHA1_HASH_SIZE = 160 / Byte.SIZE;

    public static DirectBuffer sha1(final MutableDirectBuffer to, final DirectBuffer from) {
        to.boundsCheck(0, SHA1_HASH_SIZE);
        final int size = from.capacity();
        final int blocks = size / SHA1_BLOCK_BYTES;
        final int bytesInLastBlock = size % SHA1_BLOCK_BYTES;
        final int offsetOfLastBlock = blocks * SHA1_BLOCK_BYTES;
        final int[] w = new int[80];
        final int padBlocks = (bytesInLastBlock > (SHA1_BLOCK_BYTES - Byte.BYTES - Long.BYTES) ? 2 : 1);
        final UnsafeBuffer pad = new UnsafeBuffer(new byte[SHA1_BLOCK_BYTES * padBlocks]);
        pad.putBytes(0, from, offsetOfLastBlock, bytesInLastBlock);
        pad.putByte(bytesInLastBlock, (byte) 0x80);
        pad.putLong(pad.capacity() - Long.BYTES, Byte.SIZE * size, ByteOrder.BIG_ENDIAN);

        int h0 = 0x67452301;
        int h1 = 0xEFCDAB89;
        int h2 = 0x98BADCFE;
        int h3 = 0x10325476;
        int h4 = 0xC3D2E1F0;

        for (int i = 0; i < blocks + padBlocks; i++) {
            final DirectBuffer block =
                i == blocks
                ? new UnsafeBuffer(pad, 0, SHA1_BLOCK_BYTES)
                : i == blocks + 1
                ? new UnsafeBuffer(pad, SHA1_BLOCK_BYTES, SHA1_BLOCK_BYTES)
                : new UnsafeBuffer(from, SHA1_BLOCK_BYTES * i, SHA1_BLOCK_BYTES);

            int a = h0;
            int b = h1;
            int c = h2;
            int d = h3;
            int e = h4;

            for (int j = 0; j < 16; j++) {
                w[j] = block.getInt(j * Integer.BYTES, ByteOrder.BIG_ENDIAN);

                final int fK = ((b & c) | (~(b) & d)) + 0x5A827999;
                final int temp = Integer.rotateLeft(a, 5) + fK + e + w[j];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = temp;
            }

            for (int j = 16; j <= 19; j++) {
                w[j] = Integer.rotateLeft(w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16], 1);

                final int fK = ((b & c) | (~(b) & d)) + 0x5A827999;
                final int temp = Integer.rotateLeft(a, 5) + fK + e + w[j];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = temp;
            }

            for (int j = 20; j <= 31; j++) {
                w[j] = Integer.rotateLeft(w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16], 1);

                final int fK = (b ^ c ^ d) + 0x6ED9EBA1;
                final int temp = Integer.rotateLeft(a, 5) + fK + e + w[j];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = temp;
            }

            for (int j = 32; j <= 39; j++) {
                w[j] = Integer.rotateLeft(w[j - 6] ^ w[j - 16] ^ w[j - 28] ^ w[j - 32], 2);

                final int fK = (b ^ c ^ d) + 0x6ED9EBA1;
                final int temp = Integer.rotateLeft(a, 5) + fK + e + w[j];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = temp;
            }

            for (int j = 40; j <= 59; j++) {
                w[j] = Integer.rotateLeft(w[j - 6] ^ w[j - 16] ^ w[j - 28] ^ w[j - 32], 2);

                final int fK = ((b & c) | (b & d) | (c & d)) + 0x8F1BBCDC;
                final int temp = Integer.rotateLeft(a, 5) + fK + e + w[j];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = temp;
            }

            for (int j = 60; j <= 79; j++) {
                w[j] = Integer.rotateLeft(w[j - 6] ^ w[j - 16] ^ w[j - 28] ^ w[j - 32], 2);

                final int fK = (b ^ c ^ d) + 0xCA62C1D6;
                final int temp = Integer.rotateLeft(a, 5) + fK + e + w[j];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = temp;
            }

            h0 += a;
            h1 += b;
            h2 += c;
            h3 += d;
            h4 += e;
        }

        to.putInt(0 * Integer.BYTES, h0, ByteOrder.BIG_ENDIAN);
        to.putInt(1 * Integer.BYTES, h1, ByteOrder.BIG_ENDIAN);
        to.putInt(2 * Integer.BYTES, h2, ByteOrder.BIG_ENDIAN);
        to.putInt(3 * Integer.BYTES, h3, ByteOrder.BIG_ENDIAN);
        to.putInt(4 * Integer.BYTES, h4, ByteOrder.BIG_ENDIAN);

        return new UnsafeBuffer(to, 0, SHA1_HASH_SIZE);
    }
}
