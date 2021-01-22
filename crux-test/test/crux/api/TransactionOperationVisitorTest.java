package crux.api;

import crux.api.tx.*;
import org.junit.Test;

import static crux.api.tx.Transaction.buildTx;
import static crux.api.TestUtils.*;
import static org.junit.Assert.*;

public class TransactionOperationVisitorTest {
    private final static CruxDocument document = CruxDocument.create("foo");

    private static abstract class TestVisitor implements TransactionOperation.Visitor {
        protected Object object = null;

        @Override
        public void visit(PutOperation operation) {
            throwRuntime();
        }

        @Override
        public void visit(DeleteOperation operation) {
            throwRuntime();
        }

        @Override
        public void visit(EvictOperation operation) {
            throwRuntime();
        }

        @Override
        public void visit(MatchOperation operation) {
            throwRuntime();
        }

        @Override
        public void visit(FunctionOperation operation) {
            throwRuntime();
        }

        private void throwRuntime() {
            throw new RuntimeException("We called into a method we shouldn't have!");
        }

        Object get() {
            return object;
        }
    }

    private static class ObjectWrapper {
        private Object object = null;

        Object get() {
            return object;
        }

        void set(Object object) {
            this.object = object;
        }
    }

    @Test
    public void putOnlyHitsPut() {
        TestVisitor visitor = new TestVisitor() {
            @Override
            public void visit(PutOperation operation) {
                object = operation.getDocument();
            }
        };

        Transaction transaction = buildTx(tx -> {
           tx.put(document);
        });

        transaction.visit(visitor);

        assertEquals(document, visitor.get());
    }

    @Test
    public void deleteOnlyHitsDelete() {
        TestVisitor visitor = new TestVisitor() {
            @Override
            public void visit(DeleteOperation operation) {
                object = operation.getId();
            }
        };

        Transaction transaction = buildTx(tx -> {
            tx.delete("foo");
        });

        transaction.visit(visitor);

        assertEquals("foo", visitor.get());
    }

    @Test
    public void evictOnlyHitsEvict() {
        TestVisitor visitor = new TestVisitor() {
            @Override
            public void visit(EvictOperation operation) {
                object = operation.getId();
            }
        };

        Transaction transaction = buildTx(tx -> {
            tx.evict("foo");
        });

        transaction.visit(visitor);

        assertEquals("foo", visitor.get());
    }

    @Test
    public void matchOnlyHitsMatch() {
        TestVisitor visitor = new TestVisitor() {
            @Override
            public void visit(MatchOperation operation) {
                object = operation.getCompare();
            }
        };

        Transaction transaction = buildTx(tx -> {
            tx.match(document);
        });

        transaction.visit(visitor);

        assertEquals(document, visitor.get());
    }

    @Test
    public void functionOnlyHitsFunction() {
        TestVisitor visitor = new TestVisitor() {
            @Override
            public void visit(FunctionOperation operation) {
                object = operation.getId();
            }
        };

        Transaction transaction = buildTx(tx -> {
            tx.function("foo");
        });

        transaction.visit(visitor);

        assertEquals("foo", visitor.get());
    }

    @Test(expected=RuntimeException.class)
    public void hittingInvalidMethodsThrows() {
        TestVisitor visitor = new TestVisitor() {};

        Transaction transaction = buildTx(tx -> {
            tx.put(document);
        });

        transaction.visit(visitor);
    }
}
