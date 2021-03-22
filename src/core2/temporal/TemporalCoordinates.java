package core2.temporal;

public final class TemporalCoordinates {
    public final long rowId;
    public Object id;
    public long txTimeStart;
    public long txTimeEnd;
    public long validTimeStart;
    public long validTimeEnd;
    public boolean tombstone;

    public TemporalCoordinates(long rowId) {
        this.rowId = rowId;
    }
}
