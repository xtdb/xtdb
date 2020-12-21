package crux.api.transaction.operation;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentVector;
import crux.api.document.CruxDocument;
import crux.api.document.ICruxDocument;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PutTransactionOperation extends TransactionOperation {
    private final ICruxDocument document;
    private final Date validTime;
    private final Date endValidTime;

    private PutTransactionOperation(ICruxDocument document, Date validTime, Date endValidTime) {
        super(TransactionOperation.Type.PUT);
        this.document = document;
        this.validTime = validTime;
        this.endValidTime = endValidTime;
    }

    public static PutTransactionOperation factory(PersistentVector vector) {
        Date validTime = null;
        Date endValidTime = null;

        IPersistentMap rawDocument = (IPersistentMap) vector.get(1);
        ICruxDocument document = CruxDocument.factory(rawDocument);

        if (vector.size() > 2) {
            validTime = (Date) vector.get(2);
        }

        if (vector.size() > 3) {
            endValidTime = (Date) vector.get(3);
        }

        return new PutTransactionOperation(document, validTime, endValidTime);
    }

    public static PutTransactionOperation factory(ICruxDocument document) {
        return new PutTransactionOperation(document, null, null);
    }

    public static PutTransactionOperation factory(ICruxDocument document, Date validTime) {
        return new PutTransactionOperation(document, validTime, null);
    }

    public static PutTransactionOperation factory(ICruxDocument document, Date validTime, Date endValidTime) {
        return new PutTransactionOperation(document, validTime, endValidTime);
    }

    @Override
    List<Object> getArgs() {
        ArrayList<Object> ret = new ArrayList<>();
        ret.add(document.toEdn());
        if (endValidTime != null) {
            ret.add(validTime);
            ret.add(endValidTime);
        }
        else if (validTime != null) {
            ret.add(validTime);
        }

        return ret;
    }
}
