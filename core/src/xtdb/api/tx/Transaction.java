package xtdb.api.tx;

import xtdb.api.XtdbDocument;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.function.Consumer;

public final class Transaction {
    public static Transaction buildTx(Consumer<Builder> f) {
        Builder builder = new Builder();
        f.accept(builder);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private final Iterable<TransactionOperation> operations;
    private final Date txTime;

    private Transaction(Iterable<TransactionOperation> operations, Date txTime) {
        this.operations = operations;
        this.txTime = txTime;
    }

    public Iterable<TransactionOperation> getOperations() {
        return operations;
    }

    public Date getTxTime() {
        return txTime;
    }

    public static final class Builder {
        private final ArrayList<TransactionOperation> operations = new ArrayList<>();
        private Date txTime;

        private Builder() {}

        public Builder add(TransactionOperation operation) {
            operations.add(operation);
            return this;
        }

        /**
         * Adds a put operation to the transaction, putting the given document at validTime = now.
         * @return this
         */
        public Builder put(XtdbDocument document) {
            return add(PutOperation.create(document));
        }

        /**
         * Adds a put operation to the transaction, putting the given document starting from the given valid time
         * @return this
         */
        public Builder put(XtdbDocument document, Date startValidTime) {
            return add(PutOperation.create(document, startValidTime));
        }

        /**
         * Adds a put operation to the transaction, putting the given document starting for the given validTime range
         * @return this
         */
        public Builder put(XtdbDocument document, Date startValidTime, Date endValidTime) {
            return add(PutOperation.create(document, startValidTime, endValidTime));
        }

        /**
         * Adds a delete operation to the transaction, deleting the given document from validTime = now.
         * @return this
         */
        public Builder delete(Object id) {
            return add(DeleteOperation.create(id));
        }

        /**
         * Adds a delete operation to the transaction, deleting the given document starting from the given valid time
         * @return this
         */
        public Builder delete(Object id, Date startValidTime) {
            return add(DeleteOperation.create(id, startValidTime));
        }

        /**
         * Adds a delete operation to the transaction, deleting the given document starting for the given validTime range
         * @return this
         */
        public Builder delete(Object id, Date startValidTime, Date endValidTime) {
            return add(DeleteOperation.create(id, startValidTime, endValidTime));
        }

        /**
         * Adds an evict operation to the transaction, removing all trace of the entity with the given ID.
         *
         * Eviction also removes all history of the entity - unless you are required to do this,
         * (for legal reasons, for example) we'd recommend using a {@link #delete(Object)} instead,
         * to preserve the history of the entity.
         *
         * @return this
         */
        public Builder evict(Object id) {
            return add(EvictOperation.create(id));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given entity ID does not exist at validTime = now, otherwise the transaction will abort.
         *
         * @return this
         */
        public Builder matchNotExists(Object id) {
            return add(MatchOperation.create(id));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given document is present at validTime = now, otherwise the transaction will abort.
         *
         * @return this
         */
        public Builder match(XtdbDocument document) {
            return add(MatchOperation.create(document));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given entity ID does not exist at the given validTime, otherwise the transaction will abort.
         *
         * @return this
         */
        public Builder matchNotExists(Object id, Date atValidTime) {
            return add(MatchOperation.create(id, atValidTime));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given document is present at the given valid time, otherwise the transaction will abort.
         *
         * @return this
         */
        public Builder match(XtdbDocument document, Date atValidTime) {
            return add(MatchOperation.create(document, atValidTime));
        }

        /**
         * Adds a transaction function invocation operation to the transaction.
         * Invokes the transaction function with the given id, passing it the given arguments.
         * @return this
         */
        public Builder invokeFunction(Object id, Object... arguments) {
            return add(InvokeFunctionOperation.create(id, arguments));
        }

        /**
         * Overrides the txTime of the transaction, for use in imports.
         *
         * Mustn't be older than any other transaction already submitted to XT, nor newer than the TxLog's clock.
         *
         * @param txTime overridden transaction time of this transaction.
         * @return this
         */
        public Builder withTxTime(Date txTime) {
            this.txTime = txTime;
            return this;
        }

        public Transaction build() {
            return new Transaction(operations, txTime);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return operations.equals(that.operations) && Objects.equals(txTime, that.txTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operations, txTime);
    }
}
