package core2.vector.reader;

import org.apache.arrow.memory.ArrowBuf;

public class BitUtil {
    private BitUtil() {}

    public static boolean readBoolean(ArrowBuf buf, int idx) {
        final int byteIndex = idx >> 3;
        final byte b = buf.getByte(byteIndex);
        final int bitIndex = idx & 7;
        return 1 == ((b >> bitIndex) & 0x01);
    }
}
