package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

import static crux.api.v2.Util.keyword;

public class PutOperation extends Operation {
    private static final Keyword TX_PUT = keyword("crux.tx/put");

    private final Document doc;
    private final Date validTime;

    private PutOperation(Document doc, Date validTime) {
        this.doc = doc;
        this.validTime = validTime;
    }

    public PutOperation withValidTime(Date validTime) {
        return new PutOperation(doc, validTime);
    }

    public static PutOperation putOp(Document doc) {
        return new PutOperation(doc, null);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(TX_PUT, doc.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
