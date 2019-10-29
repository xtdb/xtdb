package crux.api;

import clojure.java.api.Clojure;

public class Transaction {
    private PersistentVector transaction;

    public Transaction() {
	transaction = PersistentVector.create();
    }

    public void addOperations(Operation... operations) {
	for (Operation operation : operations) {
	    transaction = transaction.cons(operation.getOperation());
	}
    }

    public PersistentVector getTransaction () {
	return transaction;
    }
}
