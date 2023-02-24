package core2;

import java.util.Arrays;

public final class LongStack {
    private int idx;
    private long[] stack;

    public LongStack(int size) {
        this.idx = 0;
        this.stack = new long[size];
    }

    public LongStack() {
        this(16);
    }

    public void push(long x) {
        if (idx == stack.length) {
            stack = Arrays.copyOfRange(stack, 0, idx * 2);
        }
        stack[idx++] = x;
    }

    public long poll() {
        return stack[--idx];
    }

    public boolean isEmpty() {
        return idx == 0;
    }

    public int size() {
        return idx;
    }
}
