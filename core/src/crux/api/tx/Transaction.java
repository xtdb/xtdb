package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.Keyword;
import clojure.lang.PersistentVector;
import crux.api.CruxDocument;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class Transaction {
    public static Transaction buildTx(Consumer<Builder> f) {
        Builder builder = new Builder();
        f.accept(builder);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private final List<TransactionOperation> operations;

    private Transaction(List<TransactionOperation> operations) {
        this.operations = operations;
    }

    public static final class Builder {
        private final ArrayList<TransactionOperation> operations = new ArrayList<>();

        private Builder() {}

        public final Builder add(TransactionOperation operation) {
            operations.add(operation);
            return this;
        }

        /**
         * Adds a put operation to the transaction, putting the given document at validTime = now.
         * @return this
         */
        public final Builder put(CruxDocument document) {
            return add(PutOperation.create(document));
        }

        /**
         * Adds a put operation to the transaction, putting the given document starting from the given valid time
         * @return this
         */
        public final Builder put(CruxDocument document, Date startValidTime) {
            return add(PutOperation.create(document, startValidTime));
        }

        /**
         * Adds a put operation to the transaction, putting the given document starting for the given validTime range
         * @return this
         */
        public final Builder put(CruxDocument document, Date startValidTime, Date endValidTime) {
            return add(PutOperation.create(document, startValidTime, endValidTime));
        }

        /**
         * Adds a delete operation to the transaction, deleting the given document from validTime = now.
         * @return this
         */
        public final Builder delete(Object id) {
            return add(DeleteOperation.create(id));
        }

        /**
         * Adds a delete operation to the transaction, deleting the given document starting from the given valid time
         * @return this
         */
        public final Builder delete(Object id, Date startValidTime) {
            return add(DeleteOperation.create(id, startValidTime));
        }

        /**
         * Adds a delete operation to the transaction, deleting the given document starting for the given validTime range
         * @return this
         */
        public final Builder delete(Object id, Date startValidTime, Date endValidTime) {
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
        public final Builder evict(Object id) {
            return add(EvictOperation.create(id));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given entity ID does not exist at validTime = now, otherwise the transaction will abort.
         *
         * @return this
         */
        public final Builder matchNotExists(Object id) {
            return add(MatchOperation.create(id));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given document is present at validTime = now, otherwise the transaction will abort.
         *
         * @return this
         */
        public final Builder match(CruxDocument document) {
            return add(MatchOperation.create(document));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given entity ID does not exist at the given validTime, otherwise the transaction will abort.
         *
         * @return this
         */
        public final Builder matchNotExists(Object id, Date atValidTime) {
            return add(MatchOperation.create(id, atValidTime));
        }

        /**
         * Adds a match operation to the transaction.
         *
         * Asserts that the given document is present at the given valid time, otherwise the transaction will abort.
         *
         * @return this
         */
        public final Builder match(CruxDocument document, Date atValidTime) {
            return add(MatchOperation.create(document, atValidTime));
        }

        /**
         * Adds a transaction function invocation operation to the transaction.
         * Invokes the transaction function with the given id, passing it the given arguments.
         * @return this
         */
        public final Builder invokeFunction(Object id, Object... arguments) {
            return add(InvokeFunctionOperation.create(id, arguments));
        }

        public Transaction build() {
            return new Transaction(operations);
        }
    }

    private static class EdnVisitor implements TransactionOperation.Visitor<IPersistentVector> {
        private static final Keyword PUT = Keyword.intern("xt/put");
        private static final Keyword DELETE = Keyword.intern("xt/delete");
        private static final Keyword EVICT = Keyword.intern("xt/evict");
        private static final Keyword MATCH = Keyword.intern("xt/match");
        private static final Keyword FN = Keyword.intern("xt/fn");

        @Override
        public IPersistentVector visit(PutOperation operation) {
            IPersistentVector toAdd = PersistentVector.EMPTY
                    .cons(PUT)
                    .cons(operation.getDocument().toMap());

            Date startValidTime = operation.getStartValidTime();
            if (startValidTime == null) {
                return toAdd;
            }

            toAdd = toAdd.cons(startValidTime);

            Date endValidTime = operation.getEndValidTime();
            if (endValidTime == null) {
                return toAdd;
            }

            return toAdd.cons(endValidTime);
        }

        @Override
        public IPersistentVector visit(DeleteOperation operation) {
            IPersistentVector toAdd = PersistentVector.EMPTY
                    .cons(DELETE)
                    .cons(operation.getId());

            Date startValidTime = operation.getStartValidTime();
            if (startValidTime == null) {
                return toAdd;
            }

            toAdd = toAdd.cons(startValidTime);

            Date endValidTime = operation.getEndValidTime();
            if (endValidTime == null) {
                return toAdd;
            }

            return toAdd.cons(endValidTime);
        }

        @Override
        public IPersistentVector visit(EvictOperation operation) {
            return PersistentVector.EMPTY
                    .cons(EVICT)
                    .cons(operation.getId());
        }

        @Override
        public IPersistentVector visit(MatchOperation operation) {
            IPersistentVector toAdd = PersistentVector.EMPTY
                    .cons(MATCH)
                    .cons(operation.getId());

            CruxDocument document = operation.getDocument();
            if (document == null) {
                toAdd = toAdd.cons(null);
            }
            else {
                toAdd = toAdd.cons(document.toMap());
            }

            Date atValidTime = operation.getAtValidTime();
            if (atValidTime == null) {
                return toAdd;
            }

            return toAdd.cons(atValidTime);
        }

        @Override
        public IPersistentVector visit(InvokeFunctionOperation operation) {
            IPersistentVector toAdd = PersistentVector.EMPTY
                    .cons(FN)
                    .cons(operation.getId());

            for (Object argument: operation.getArguments()) {
                toAdd = toAdd.cons(argument);
            }

            return toAdd;
        }
    }

    public final IPersistentVector toVector() {
        return PersistentVector.create(accept(new EdnVisitor()));
    }

    public <E> List<E> accept(TransactionOperation.Visitor<E> visitor) {
        return operations.stream().map(it -> it.accept(visitor)).collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return operations.equals(that.operations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operations);
    }
}
