package xtdb.tx;

import clojure.lang.Keyword;

public final class Erase extends Ops {

    private final Keyword tableName;
    private final Object entityId;

    Erase(Keyword tableName, Object entityId) {
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
}
