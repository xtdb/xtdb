package xtdb.tx;

import clojure.lang.*;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TxOptions implements ILookup, Seqable {
    private static final Keyword SYSTEM_TIME_KEY = Keyword.intern("system-time");
    private static final Keyword DEFAULT_TZ_KEY = Keyword.intern("default-tz");

    private Instant systemTime;
    private ZoneId defaultTz;

    public TxOptions(Instant systemTime, ZoneId defaultTz) {
        this.systemTime = systemTime;
        this.defaultTz = defaultTz;
    }

    public Instant systemTime() {
        return systemTime;
    }

    public ZoneId defaultTz() {
        return defaultTz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxOptions txOptions = (TxOptions) o;
        return Objects.equals(systemTime, txOptions.systemTime) && Objects.equals(defaultTz, txOptions.defaultTz);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemTime, defaultTz);
    }

    @Override
    public String toString() {
        return "TxOptions{" +
                "systemTime=" + systemTime +
                ", defaultTz=" + defaultTz +
                '}';
    }

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }

    @Override
    public Object valAt(Object key, Object notFound) {
        if (key == SYSTEM_TIME_KEY) {
            return systemTime;
        } else if (key == DEFAULT_TZ_KEY) {
            return defaultTz;
        } else {
            return notFound;
        }
    }

    @Override
    public ISeq seq() {
        List<Object> seqList = new ArrayList<>();
        if (systemTime != null) {
            seqList.add(MapEntry.create(SYSTEM_TIME_KEY, systemTime));
        }
        if (defaultTz != null) {
            seqList.add(MapEntry.create(DEFAULT_TZ_KEY, defaultTz));
        }
        return PersistentList.create(seqList).seq();
    }
}