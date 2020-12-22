package crux.api.transaction.operation;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentVector;
import crux.api.document.CruxDocument;
import crux.api.document.ICruxDocument;
import crux.api.exception.CruxIdException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MatchTransactionOperation extends TransactionOperation {
    private final Object id;
    private final ICruxDocument document;
    private final Date validTime;

    private MatchTransactionOperation(Object id, ICruxDocument document, Date validTime) {
        super(Type.MATCH);
        CruxIdException.assertValidType(id);
        this.id = id;
        this.document = document;
        this.validTime = validTime;
    }

    public static MatchTransactionOperation factory(PersistentVector vector) {
        Object id = vector.get(1);

        IPersistentMap rawDocument = (IPersistentMap) vector.get(2);
        ICruxDocument document = CruxDocument.factory(rawDocument);

        Date validTime = null;
        if (vector.size() > 3) {
            validTime = (Date) vector.get(3);
        }

        return new MatchTransactionOperation(id, document, validTime);
    }

    public static MatchTransactionOperation factoryNotExists(Object id) {
        return factoryNotExists(id, null);
    }

    public static MatchTransactionOperation factoryNotExists(Object id, Date validTime) {
        return new MatchTransactionOperation(id, null, validTime);
    }

    public static MatchTransactionOperation factory(Object id, ICruxDocument document) {
        return factory(id, document, null);
    }

    public static MatchTransactionOperation factory(Object id, ICruxDocument document, Date validTime) {
        return new MatchTransactionOperation(id, document, validTime);
    }

    @Override
    List<Object> getArgs() {
        ArrayList<Object> ret = new ArrayList<>();
        ret.add(id);

        if (document == null) {
            ret.add(null);
        }
        else {
            ret.add(document.toEdn());
        }

        if (validTime != null) {
            ret.add(validTime);
        }

        return ret;
    }
}
