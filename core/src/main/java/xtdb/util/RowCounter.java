package xtdb.util;

public class RowCounter {
    /**
     * An approximation of the chunk-size overhead added for a single row, regardless of contents.
     * This number is a parameter to the deterministic indexing, so it is not something we can change.
     */
    private static final long ROW_BYTE_COUNT_APPROX_OVERHEAD = 48;

    private long chunkIdx;
    private long chunkRowCount;
    private long chunkApproxByteCount;

    public RowCounter(long chunkIdx) {
        this.chunkIdx = chunkIdx;
    }

    public void nextChunk() {
        chunkIdx += chunkRowCount;
        chunkRowCount = 0;
        chunkApproxByteCount = 0;
    }

    public void addRows(int rowCount) {
        chunkRowCount += rowCount;
        chunkApproxByteCount += ROW_BYTE_COUNT_APPROX_OVERHEAD * rowCount;
    }
    public void addTxBytes(int txByteCount) { chunkApproxByteCount += txByteCount; }

    public long getChunkRowCount() {
        return chunkRowCount;
    }

    public long getChunkIdx() {
        return chunkIdx;
    }

    public long getChunkByteCount() {
        return chunkApproxByteCount;
    }
}
