package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Map;

public abstract class AbstractCruxDocument {
    private static final String DB_ID_RAW = "crux.db/id";
    public static final Keyword DB_ID = Keyword.intern(DB_ID_RAW);

    protected abstract Map<Keyword, Object> getData();
    public abstract Object getId();

    public final IPersistentMap toMap() {
        return PersistentArrayMap.create(getData())
                .assoc(DB_ID, getId());
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof AbstractCruxDocument)) return false;
        return toMap().equals(((AbstractCruxDocument) o).toMap());
    }

    @Override
    public final int hashCode() {
        return toMap().hashCode();
    }
}
