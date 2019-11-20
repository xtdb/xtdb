package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

import static crux.api.v2.Util.keyword;

public class CasOperation extends Operation {
    private static final Keyword TX_CAS = keyword("crux.tx/cas");

    private final Date validTime;
    private final Document oldDoc;
    private final Document newDoc;

    private CasOperation(Document oldDoc, Document newDoc, Date validTime) {
        this.oldDoc = oldDoc;
        this.newDoc = newDoc;
        this.validTime = validTime;
    }

    public CasOperation withValidTime(Date validTime) {
        return new CasOperation(oldDoc, newDoc, validTime);
    }

    public static CasOperation casOp(Document oldDoc, Document newDoc) {
        return new CasOperation(oldDoc, newDoc, null);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(TX_CAS, oldDoc.toEdn(), newDoc.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
