package core2;

import java.nio.ByteBuffer;

public class StringUtil {

    /**
     * Returns the 1-based position of the given (needle) bytes in the haystack. 0 if not found.
     * This is equivalent to sql POSITION on binary.
     */
    public static int sqlPosition(ByteBuffer needle, ByteBuffer haystack) {
        if(needle.remaining() == 0) return 1;
        int i = 0;
        int j = 0;
        while(true) {
            if (j == needle.remaining()) return i+1;
            if (i + j == haystack.remaining()) return 0;
            if (haystack.get(i + j) == needle.get(j)) {
                j++;
            } else {
                i++;
                j = 0;
            }
        }
    }

    public static boolean isUtf8Char(byte b) {
        return (b & 0xc0) != 0x80;
    }

    /**
     * In our SQL dialect & arrow we operate always on utf8.
     * Java strings are utf-16 backed, and java chars cannot represent all utf-8 characters in one char - therefore
     * we do not use string length or anything like it, so that one character == one code point.
     * */
    public static int utf8Length(ByteBuffer buf, int start, int end) {
        int len = 0;
        for(int i = start; i < end; i++) {
            if (isUtf8Char(buf.get(i))) {
                len++;
            }
        }
        return len;
    }

    public static int utf8Length(ByteBuffer buf) {
        return utf8Length(buf, 0, buf.remaining());
    }

    /**
     * Returns the 1-based codepoint position of the needle in the haystack. 0 if not found.
     * This is equivalent to sql POSITION on characters.
     * */
    public static int sqlUtf8Position(ByteBuffer needle, ByteBuffer haystack) {
        int bpos = sqlPosition(needle, haystack);
        if (bpos == 0) return 0;
        return utf8Length(haystack, 0, bpos-1) + 1;
    }

    public static ByteBuffer sqlSubstring(ByteBuffer target, int pos, int len, boolean useLen){
        if (useLen && len < 0) {
            throw new java.lang.IllegalArgumentException("Negative substring length");
        }

        if (useLen && len == 0) {
            return ByteBuffer.allocate(0);
        }

        int startCodepoint = Math.max(pos-1, 0);
        int startIndex = 0;

        int cp = -1;
        for(int i = target.position(); i < target.remaining() && cp < startCodepoint; i++) {
            if(isUtf8Char(target.get(i))) {
                cp++;
            }
            startIndex = i;
        }

        if (cp < startCodepoint) {
            return ByteBuffer.allocate(0);
        }

        if (!useLen) {
            ByteBuffer ret = target.duplicate();
            ret.position(startIndex);
            return ret;
        }

        int charsConsumed = 0;
        for(int i = startIndex; i < target.remaining(); i++) {

            if (charsConsumed == len && isUtf8Char(target.get(i))) {
                ByteBuffer ret = target.duplicate();
                ret.position(startIndex);
                ret.limit(i);
                return ret;
            }

            if (isUtf8Char(target.get(i))) {
                charsConsumed++;
            }
        }

        ByteBuffer ret = target.duplicate();
        ret.position(startIndex);
        return ret;
    }

    public static ByteBuffer sqlOverlay(ByteBuffer target, ByteBuffer placing, int start, int replaceLength) {
        ByteBuffer s1 = sqlSubstring(target, 1, start-1, true);
        ByteBuffer s2 = sqlSubstring(target, start + replaceLength, -1, false);

        ByteBuffer newBuf = ByteBuffer.allocate(s1.remaining() + s2.remaining() + placing.remaining());
        newBuf.put(s1);
        newBuf.put(placing.duplicate());
        newBuf.put(s2);
        newBuf.position(0);
        return newBuf;
    }
}
