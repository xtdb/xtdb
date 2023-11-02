package xtdb.bitemporal;

public interface IRowConsumer {
    void accept(int idx, long validFrom, long validTo, long systemFrom, long systemTo);
}
