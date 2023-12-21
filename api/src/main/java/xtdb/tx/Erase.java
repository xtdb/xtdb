package xtdb.tx;

import clojure.lang.Keyword;

import java.util.Objects;

public final class Erase extends Ops {

    private final Keyword tableName;
    private final Object entityId;

    public Erase(Keyword tableName, Object entityId) {
        this.tableName = tableName;
        this.entityId = entityId;
    }

    public Keyword tableName() {
        return tableName;
    }

    public Object entityId() {
        return entityId;
    }

    @Override
    public String toString() {
        return String.format("[:erase {tableName=%s, entityId=%s}]", tableName, entityId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Erase erase = (Erase) o;
        return Objects.equals(tableName, erase.tableName) && Objects.equals(entityId, erase.entityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, entityId);
    }
}
