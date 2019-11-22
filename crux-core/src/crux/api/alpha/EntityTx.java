package crux.api.alpha;

import clojure.lang.Keyword;

import java.util.Date;
import java.util.Map;

public class EntityTx {
    public final String id;
    public final String contentHash;
    public final Date validTime;
    public final Date txTime;
    public final long txId;

    @Override
    public String toString() {
        return "EntityTx{" +
            "id='" + id + '\'' +
            ", contentHash='" + contentHash + '\'' +
            ", validTime=" + validTime +
            ", txTime=" + txTime +
            ", txId=" + txId +
            '}';
    }

    private EntityTx(String id, String contentHash, Date validTime, Date txTime, long txId) {
        this.id = id;
        this.contentHash = contentHash;
        this.validTime = validTime;
        this.txTime = txTime;
        this.txId = txId;
    }

    @SuppressWarnings("unchecked")
    static EntityTx entityTx(Map<Keyword,?> entityTx) {
        String id = (String) entityTx.get(Util.keyword("crux.db/id" ));
        String contentHash = (String) entityTx.get(Util.keyword("crux.db/content-hash" ));
        Date validTime = (Date) entityTx.get(Util.keyword("crux.db/valid-time" ));
        Date txTime = (Date) entityTx.get(Util.keyword("crux.tx/tx-time" ));
        Long txId = (Long) entityTx.get(Util.keyword("crux.tx/tx-id" ));
        return new EntityTx(id, contentHash, validTime, txTime, txId);
    }
}
