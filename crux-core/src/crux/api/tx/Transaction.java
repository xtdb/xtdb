package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import crux.api.AbstractCruxDocument;

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

        public final Builder put(AbstractCruxDocument document) {
            return add(PutOperation.create(document));
        }

        public final Builder put(AbstractCruxDocument document, Date validTime) {
            return add(PutOperation.create(document, validTime));
        }

        public final Builder put(AbstractCruxDocument document, Date validTime, Date endValidTime) {
            return add(PutOperation.create(document, validTime, endValidTime));
        }

        public final Builder delete(Object id) {
            return add(DeleteOperation.create(id));
        }

        public final Builder delete(Object id, Date validTime) {
            return add(DeleteOperation.create(id, validTime));
        }

        public final Builder delete(Object id, Date validTime, Date endValidTime) {
            return add(DeleteOperation.create(id, validTime, endValidTime));
        }

        public final Builder evict(Object id) {
            return add(EvictOperation.create(id));
        }

        public final Builder matchNotExists(Object id) {
            return add(MatchOperation.create(id));
        }

        public final Builder match(AbstractCruxDocument document) {
            return add(MatchOperation.create(document));
        }

        public final Builder matchNotExists(Object id, Date validTime) {
            return add(MatchOperation.create(id, validTime));
        }

        public final Builder match(AbstractCruxDocument document, Date validTime) {
            return add(MatchOperation.create(document, validTime));
        }

        public Transaction build() {
            return new Transaction(operations);
        }
    }

    public final IPersistentVector toVector() {
        return PersistentVector.create(
                operations.stream().map(TransactionOperation::toVector)
                        .collect(Collectors.toList())
        );
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
