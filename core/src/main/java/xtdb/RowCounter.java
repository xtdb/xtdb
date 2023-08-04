package xtdb;

public class RowCounter {
    private long chunkIdx;
    private long chunkRowCount;

    public RowCounter(long chunkIdx) {
        this.chunkIdx = chunkIdx;
    }

    public void nextChunk() {
        chunkIdx += chunkRowCount;
        chunkRowCount = 0;
    }

    public void addRows(int rowCount) {
        chunkRowCount += rowCount;
    }

    public long getChunkRowCount() {
        return chunkRowCount;
    }

    public long getChunkIdx() {
        return chunkIdx;
    }
}
