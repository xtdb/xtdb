package crux.api.alphav2.transaction.operation;

import clojure.lang.PersistentVector;
import crux.api.alphav2.CruxId;

import java.util.Date;
import java.util.List;

public class DeleteTransactionOperation extends TransactionOperation {
    private final CruxId id;
    private final Date validTime;

    private DeleteTransactionOperation(CruxId id, Date validTime) {
        super(Type.DELETE);
        this.id = id;
        this.validTime = validTime;
    }

    public static DeleteTransactionOperation factory(PersistentVector vector) {
        Object rawId = vector.get(1);
        CruxId id = CruxId.cruxId(rawId);
        Date validTime = (Date) vector.get(2);
        return new DeleteTransactionOperation(id, validTime);
    }

    public static DeleteTransactionOperation factory(CruxId id, Date validTime) {
        return new DeleteTransactionOperation(id, validTime);
    }

    @Override
    List<Object> getArgs() {
        return List.of(id, validTime);
    }
}
