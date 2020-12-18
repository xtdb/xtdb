package crux.api.alphav2.transaction.operation;

import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import crux.api.alphav2.CruxDocument;
import crux.api.alphav2.ICruxDocument;

import java.util.Date;
import java.util.List;

public class PutTransactionOperation extends TransactionOperation {
    private final ICruxDocument document;
    private final Date validTime;
    private final Date endValidTime;

    public PutTransactionOperation(ICruxDocument document, Date validTime, Date endValidTime) {
        super(TransactionOperation.Type.PUT);
        this.document = document;
        this.validTime = validTime;
        this.endValidTime = endValidTime;
    }

    public static PutTransactionOperation factory(PersistentVector vector) {
        Date validTime = null;
        Date endValidTime = null;

        PersistentArrayMap rawDocument = (PersistentArrayMap) vector.get(1);
        ICruxDocument document = new CruxDocument(rawDocument);

        if (vector.size() > 2) {
            validTime = (Date) vector.get(2);
        }

        if (vector.size() > 3) {
            endValidTime = (Date) vector.get(3);
        }

        return new PutTransactionOperation(document, validTime, endValidTime);
    }

    @Override
    List<Object> getArgs() {
        if (endValidTime != null) {
            return List.of(document, validTime, endValidTime);
        }
        else if (validTime != null) {
            return List.of(document, validTime);
        }
        else {
            return List.of(document);
        }
    }
}
