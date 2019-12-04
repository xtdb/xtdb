package crux.api.alpha;

import clojure.lang.Keyword;

import java.util.Map;

public class EntityTxWithDocument {
    public final EntityTx entityTx;
    public Document document;

    @Override
    public String toString() {
        return "EntityTxWithDocument{" +
            "entityTx=" + entityTx +
            ", document=" + document +
            '}';
    }

    private EntityTxWithDocument(EntityTx entityTx, Document document) {
        this.entityTx = entityTx;
        this.document = document;
    }

    @SuppressWarnings("unchecked")
    static EntityTxWithDocument entityTxWithDocument(Map<Keyword, ?> entityTxWithDoc) {
        Map<Keyword, Object> doc = (Map<Keyword, Object>) entityTxWithDoc.get(Util.keyword("crux.db/doc"));
        entityTxWithDoc.remove(Util.keyword("crux.db/doc"));
        EntityTx entityTx = EntityTx.entityTx(entityTxWithDoc);
        return new EntityTxWithDocument(entityTx, Document.document(doc));
    }
}
