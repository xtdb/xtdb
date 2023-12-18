package xtdb.tx;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

public class TxOptions {
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
}
