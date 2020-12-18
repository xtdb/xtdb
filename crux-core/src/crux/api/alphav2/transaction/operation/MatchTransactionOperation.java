package crux.api.alphav2.transaction.operation;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import crux.api.alphav2.CruxDocument;
import crux.api.alphav2.CruxId;
import crux.api.alphav2.ICruxDocument;

import java.util.Date;
import java.util.List;

public class MatchTransactionOperation extends TransactionOperation {
    private final CruxId id;
    private final ICruxDocument document;
    private final Date validTime;

    public MatchTransactionOperation(CruxId id, ICruxDocument document, Date validTime) {
        super(Type.MATCH);
        this.id = id;
        this.document = document;
        this.validTime = validTime;
    }

    public static MatchTransactionOperation factory(PersistentVector vector) {
        Object rawId = vector.get(1);
        CruxId id = CruxId.cruxId(rawId);

        PersistentArrayMap rawDocument = (PersistentArrayMap) vector.get(2);
        ICruxDocument document = new CruxDocument(rawDocument);

        Date validTime = null;
        if (vector.size() > 3) {
            validTime = (Date) vector.get(3);
        }

        return new MatchTransactionOperation(id, document, validTime);
    }


    @Override
    List<Object> getArgs() {
        if (validTime != null) {
            return List.of(id, document, validTime);
        }
        else {
            return List.of(id, document);
        }
    }
}
