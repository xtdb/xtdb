package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

import static crux.api.alpha.Util.keyword;

public class DeleteOperation extends TransactionOperation {
    private static final Keyword TX_DELETE = keyword("crux.tx/delete");

    private final Date validTime;
    private final CruxId deleteId;

    DeleteOperation(CruxId deleteId, Date validTime) {
        this.deleteId = deleteId;
        this.validTime = validTime;
    }

    public DeleteOperation withValidTime(Date validTime) {
        return new DeleteOperation(deleteId, validTime);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(TX_DELETE, deleteId.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
