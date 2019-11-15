package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

public class CasOperation extends Operation {
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
        PersistentVector outputVector = PersistentVector.create(Keyword.intern("crux.tx/cas"), oldDoc.toEdn(), newDoc.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
