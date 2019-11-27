package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;
import java.util.List;

import static crux.api.alpha.Util.keyword;

public class PutOperation extends TransactionOperation {
    private final Document doc;
    private final Date validTime;

    PutOperation(Document doc, Date validTime) {
        this.doc = doc;
        this.validTime = validTime;
    }

    public PutOperation withValidTime(Date validTime) {
        return new PutOperation(doc, validTime);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(TX_PUT, doc.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
