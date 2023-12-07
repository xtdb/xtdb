package xtdb.tx;

import clojure.lang.Keyword;

import java.time.Instant;
import java.util.Objects;

public final class Delete extends Ops implements Ops.HasValidTimeBounds<Delete> {

    private final Keyword tableName;
    private final Object entityId;
    private final Instant validFrom;
    private final Instant validTo;

    Delete(Keyword tableName, Object entityId) {
        this(tableName, entityId, null, null);
    }

    private Delete(Keyword tableName, Object entityId, Instant validFrom, Instant validTo) {
        this.tableName = Objects.requireNonNull(tableName, "expected tableName");
        this.entityId = Objects.requireNonNull(entityId, "expected entityId");
        this.validFrom = validFrom;
        this.validTo = validTo;
    }

    public Keyword tableName() {
        return tableName;
    }

    public Object entityId() {
        return entityId;
    }

    public Instant validFrom() {
        return validFrom;
    }

    @Override
    public xtdb.tx.Delete startingFrom(Instant validFrom) {
        return new xtdb.tx.Delete(tableName, entityId, validFrom, validTo);
    }

    public Instant validTo() {
        return validTo;
    }

    @Override
    public xtdb.tx.Delete until(Instant validTo) {
        return new xtdb.tx.Delete(tableName, entityId, validFrom, validTo);
    }

    @Override
    public xtdb.tx.Delete during(Instant validFrom, Instant validTo) {
        return new xtdb.tx.Delete(tableName, entityId, validFrom, validTo);
    }

    @Override
    public String toString() {
        return String.format("[:delete %s %s {validFrom=%s, validTo=%s}]", tableName, entityId, validFrom, validTo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Delete delete = (Delete) o;
        return Objects.equals(tableName, delete.tableName) && Objects.equals(entityId, delete.entityId) && Objects.equals(validFrom, delete.validFrom) && Objects.equals(validTo, delete.validTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, entityId, validFrom, validTo);
    }
}
