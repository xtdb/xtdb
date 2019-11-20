package crux.api.alpha;

import clojure.lang.PersistentVector;

public abstract class TransactionOperation {
    TransactionOperation() {
    }

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

    protected abstract PersistentVector toEdn();
}
