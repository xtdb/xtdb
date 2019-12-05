package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static crux.api.alpha.CruxId.cruxId;
import static crux.api.alpha.Document.document;
import static crux.api.alpha.Util.keyword;

public abstract class TransactionOperation {
    static final Keyword TX_PUT = keyword("crux.tx/put");
    static final Keyword TX_EVICT = keyword("crux.tx/evict");
    static final Keyword TX_CAS = keyword("crux.tx/cas");
    static final Keyword TX_DELETE = keyword("crux.tx/delete");

    TransactionOperation() {
    }

    // May want to move these - are inherited by the classes
    public static DeleteOperation deleteOp(CruxId deleteId) {
        return new DeleteOperation(deleteId, null);
    }

    public static CasOperation casOp(Document oldDoc, Document newDoc) {
        return new CasOperation(oldDoc, newDoc, null);
    }

    public static EvictOperation evictOp(CruxId evictId) {
        return new EvictOperation(evictId);
    }

    public static PutOperation putOp(Document doc) {
        return new PutOperation(doc, null);
    }
    @SuppressWarnings("unchecked")
    static TransactionOperation fromEdn(PersistentVector operation) {
        Keyword opType = (Keyword) operation.nth(0);

        if (opType == TX_PUT) { ;
            return new PutOperation(
                document((Map<Keyword, Object>) operation.nth(1)),
                (Date) operation.nth(2, null));
        } else if(opType == TX_CAS) {
            return new CasOperation(
                document((Map<Keyword, Object>) operation.nth(1)),
                document((Map<Keyword, Object>) operation.nth(2)),
                (Date) operation.nth(3, null));
        } else if(opType == TX_DELETE) {
            return new DeleteOperation(
                cruxId(operation.get(1)),
                (Date) operation.nth(2, null));
        } else if(opType == TX_EVICT) {
            return new EvictOperation(
                cruxId(operation.get(1)));
        }

        return null;
    }

    protected abstract PersistentVector toEdn();
}
