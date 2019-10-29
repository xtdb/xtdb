package crux.api;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class DeleteOperation implements Operation {
    private PersistentVector operation;
    private Object deleteId;
    private Date validTime = null;

    public DeleteOperation() {
	operation = PersistentVector.create();
	operation = operation.cons(Keyword.intern("crux.tx/delete"));
    }

    public void putId(String id) {
	deleteId = Keyword.intern(id);
    }

    public void putId(UUID id) {
	deleteId = id;
    }

    public void putId(URL id) {
	deleteId = id;
    }

    public void putId(URI id) {
	deleteId = id;
    }

    public void putValidTime(Date validtime) {
	validTime = validtime;
	validTimeSet = true;
    }

    public PersistentVector getOperation() {
	operation = operation.cons(deleteId);
	if (validTime)
	    operation = operation.cons(validTime);
	return operation;
    }
}
