package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.Date;

import static crux.api.v2.Util.kw;

public class DeleteOperation extends Operation {
    private static final Keyword TX_DELETE = kw("crux.tx/delete");

    private final Date validTime;
    private final CruxId deleteId;

    private DeleteOperation(CruxId deleteId, Date validTime) {
        this.deleteId = deleteId;
        this.validTime = validTime;
    }

    public DeleteOperation withValidTime(Date validTime) {
        return new DeleteOperation(deleteId, validTime);
    }

    public static DeleteOperation deleteOp(CruxId deleteId) {
        return new DeleteOperation(deleteId, null);
    }

    @Override
    protected PersistentVector toEdn() {
        PersistentVector outputVector = PersistentVector.create(TX_DELETE, deleteId.toEdn());
        if(validTime != null)
            outputVector = outputVector.cons(validTime);
        return outputVector;
    }
}
