package crux.api.alphav2.transaction;

import clojure.lang.PersistentVector;
import crux.api.alphav2.CruxId;
import crux.api.alphav2.transaction.operation.*;
import crux.api.alphav2.IBuilder;
import crux.api.alphav2.ICruxDocument;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Transaction {
    public static Transaction build(Consumer<Builder> f) {
        Builder b = new Builder();
        f.accept(b);
        return b.build();
    }

    public static Transaction factory(PersistentVector vector) {
        ArrayList<TransactionOperation> transactionOperations = new ArrayList<>();
        for (Object rawTransaction: vector) {
            PersistentVector vectorTransaction = (PersistentVector) rawTransaction;
            TransactionOperation transactionOperation = TransactionOperation.factory(vectorTransaction);
            transactionOperations.add(transactionOperation);
        }
        return new Transaction(transactionOperations);
    }

    public static class Builder implements IBuilder<Transaction> {
        private final ArrayList<TransactionOperation> transactionOperations = new ArrayList<>();

        public Builder add(TransactionOperation transactionOperation) {
            transactionOperations.add(transactionOperation);
            return this;
        }

        public Builder put(ICruxDocument document) {
            return put(document, null);
        }

        public Builder put(ICruxDocument document, Date validTime) {
            return put(document, validTime, null);
        }

        public Builder put(ICruxDocument document, Date validTime, Date endValidTime) {
            return add( new PutTransactionOperation(document, validTime, endValidTime));
        }

        public Builder match(ICruxDocument document, Date validTime) {
            CruxId id = document.getDocumentId();
            return match(id, document, validTime);
        }

        public Builder empty(CruxId id, Date validTime) {
            return match(id, null, validTime);
        }

        public Builder match(CruxId id, ICruxDocument document, Date validTime) {
            return add( new MatchTransactionOperation(id, document, validTime));
        }

        public Builder delete(ICruxDocument document, Date validTime) {
            return delete(document.getDocumentId(), validTime);
        }

        public Builder delete(CruxId id, Date validTime) {
            return add(new DeleteTransactionOperation(id, validTime));
        }

        public Builder evict(ICruxDocument document) {
            return evict(document.getDocumentId());
        }

        public Builder evict(CruxId id) {
            return add( new EvictTransactionOperation(id));
        }

        @Override
        public Transaction build() {
            return new Transaction(transactionOperations);
        }
    }

    private final List<TransactionOperation> transactionOperations;

    private Transaction(List<TransactionOperation> transactionOperations) {
        this.transactionOperations = transactionOperations;
    }


    public PersistentVector toEdn() {
        List<PersistentVector> persistentVectors = transactionOperations.stream()
                .map(TransactionOperation::toEdn)
                .collect(Collectors.toList());
        return PersistentVector.create(persistentVectors);
    }
}
