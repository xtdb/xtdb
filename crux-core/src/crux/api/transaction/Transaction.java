package crux.api.transaction;

import clojure.lang.PersistentVector;
import crux.api.IBuilder;
import crux.api.document.ICruxDocument;
import crux.api.transaction.operation.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Transaction {
    public static Transaction build(Consumer<Builder> f) {
        Builder b = new Builder();
        f.accept(b);
        return b.build();
    }

    public static Transaction factory(Object[] vector) {
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
            return add(PutTransactionOperation.factory(document));
        }

        public Builder put(ICruxDocument document, Date validTime) {

            return add(PutTransactionOperation.factory(document, validTime));
        }

        public Builder put(ICruxDocument document, Date validTime, Date endValidTime) {
            return add( PutTransactionOperation.factory(document, validTime, endValidTime));
        }

        public Builder match(ICruxDocument document) {
            return match(document, null);
        }

        public Builder match(ICruxDocument document, Date validTime) {
            Object id = document.getDocumentId();
            return add(MatchTransactionOperation.factory(id, document, validTime));
        }

        public Builder matchNotExists(Object id) {
            return add(MatchTransactionOperation.factoryNotExists(id));
        }

        public Builder matchNotExists(Object id, Date validTime) {
            return add(MatchTransactionOperation.factoryNotExists(id, validTime));
        }

        public Builder match(Object id, ICruxDocument document, Date validTime) {
            return add(MatchTransactionOperation.factory(id, document, validTime));
        }

        public Builder delete(Object id) {
            return add(DeleteTransactionOperation.factory(id));
        }

        public Builder delete(Object id, Date validTime) {
            return add(DeleteTransactionOperation.factory(id, validTime));
        }

        public Builder delete(Object id, Date validTime, Date endValidTime) {
            return add(DeleteTransactionOperation.factory(id, validTime, endValidTime));
        }

        public Builder evict(Object id) {
            return add( EvictTransactionOperation.factory(id));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return transactionOperations.equals(that.transactionOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionOperations);
    }
}
