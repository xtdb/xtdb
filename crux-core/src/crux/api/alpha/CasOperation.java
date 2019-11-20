package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

import static crux.api.alpha.Util.keyword;

public class CasOperation extends TransactionOperation {
    private static final Keyword TX_CAS = keyword("crux.tx/cas");

    private final Date validTime;
    private final Document oldDoc;
    private final Document newDoc;

    CasOperation(Document oldDoc, Document newDoc, Date validTime) {
        this.oldDoc = oldDoc;
        this.newDoc = newDoc;
        this.validTime = validTime;
    }

    public CasOperation withValidTime(Date validTime) {
        return new CasOperation(oldDoc, newDoc, validTime);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(TX_CAS, oldDoc.toEdn(), newDoc.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
