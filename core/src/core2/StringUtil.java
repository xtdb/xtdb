package core2;

import java.nio.ByteBuffer;

public class StringUtil {

    /**
     * Returns the 1-based position of the given (needle) bytes in the haystack. 0 if not found.
     * This is equivalent to sql POSITION on binary.
     */
    public static int sqlPosition(ByteBuffer needle, ByteBuffer haystack) {
        if(needle.limit() == 0) return 1;
        int i = 0;
        int j = 0;
        while(true) {
            if (j == needle.limit()) return i+1;
            if (i + j == haystack.limit()) return 0;
            if (haystack.get(i+j) == needle.get(j)) {
                j++;
            } else {
                i++;
                j = 0;
            }
        }
    }

    /**
     * In our SQL dialect & arrow we operate always on utf8.
     * Java strings are utf-16 backed, and java chars cannot represent all utf-8 characters in one char - therefore
     * we do not use string length or anything like it, so that one character == one code point.
     * */
    public static int utf8Length(ByteBuffer buf, int start, int end) {
        int len = 0;
        for(int i = start; i < end; i++) {
            if ((buf.get(i) & 0xc0) != 0x80) {
                len++;
            }
        }
        return len;
    }

    public static int utf8Length(ByteBuffer buf) {
        return utf8Length(buf, 0, buf.limit());
    }

    /**
     * Returns the 1-based codepoint position of the needle in the haystack. 0 if not found.
     * This is equivalent to sql POSITION on characters.
     * */
    public static int sqlUtf8Position(ByteBuffer needle, ByteBuffer haystack) {
        int bpos = sqlPosition(needle, haystack);
        if (bpos == 0) return 0;
        return utf8Length(haystack, 0, bpos-1)+1;
    }
}
