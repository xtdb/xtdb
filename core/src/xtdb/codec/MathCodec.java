package xtdb.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.math.BigDecimal;
import java.math.BigInteger;

@SuppressWarnings("unused")
public class MathCodec {

    public static int encodeExponent(BigDecimal x) {
        return Integer.MIN_VALUE ^ (x.signum() * (x.precision() - x.scale()));
    }

    private static int binaryCodedChar(String s, int idx) {
        return idx < s.length() ? Character.digit(s.charAt(idx), 10) + 1 : 0;
    }

    public static int putBinaryCodedDecimal(int signum, String s, MutableDirectBuffer buf, int offset) {
        int chars = s.length();
        int mask = signum < 0 ? 0xff : 0;
        int bytes = (chars / 2) + 1;

        for (int i = 0; i < bytes; i++) {
            int c1 = binaryCodedChar(s, i * 2);
            int c2 = binaryCodedChar(s, i * 2 + 1);
            buf.putByte(i + offset, (byte) (mask ^ (c1 << 4 | c2)));
        }

        return bytes;
    }

    public static String getBinaryCodedDecimal(DirectBuffer buf, int signum) {
        int mask = signum < 0 ? 0xff : 0;

        boolean lastDigitZero = 0 == ((mask ^ buf.getByte(buf.capacity() - 1)) & 0xf);
        int precision = (buf.capacity() * 2) - (lastDigitZero ? 1 : 0);

        StringBuilder sb = new StringBuilder(precision);
        for (int i = 0; i < buf.capacity(); i++) {
            byte b = (byte) (mask ^ buf.getByte(i));
            int nibble = (0xf & (b >> 4)) - 1;
            if (nibble >= 0) sb.append(nibble);
            nibble = (0xf & b) - 1;
            if (nibble >= 0) sb.append(nibble);
        }

        return sb.toString();
    }

    public static BigDecimal decodeBigDecimal(int signum, int encodedExponent, String decodedBCD) {
        BigInteger bigInt = new BigInteger(decodedBCD);
        if (signum < 0) bigInt = bigInt.negate();

        int scale = decodedBCD.length() - signum * (Integer.MIN_VALUE ^ encodedExponent);

        return new BigDecimal(bigInt, scale);
    }
}
