package crux.api;

import clojure.lang.PersistentVector;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class DeleteOperation implements Operation {
    private PersistentVector operation;
    private Object deleteId;
    private Date validTime;

    public static class Builder implements OperationBuilder {
	private PersistentVector operation;
	private Object deleteId;
	private Date validTime;

	private void init() {
	    operation = PersistentVector.create();
	    operation = operation.cons(Keyword.intern("crux.tx/delete"));
	}

	public Builder(String id) {
	    init();
	    putId(id);
	}

	public Builder(UUID id) {
	    init();
	    putId(id);
	}

	public Builder(URL id) {
	    init();
	    putId(id);
	}

	public Builder(URI id) {
	    init();
	    putId(id);
	}

	public Builder putId(String id) {
	    deleteId = Keyword.intern(id);
	    return this;
	}

	public Builder putId(UUID id) {
	    deleteId = id;
	    return this;
	}

	public Builder putId(URL id) {
	    deleteId = id;
	    return this;
	}

	public Builder putId(URI id) {
	    deleteId = id;
	    return this;
	}

	public Builder putValidTime(Date validtime) {
	    validTime = validtime;
	    return this;
	}

	public DeleteOperation build() {
	    DeleteOperation deleteOp = new DeleteOperation();
	    deleteOp.operation = operation;
	    deleteOp.deleteId = deleteId;
	    deleteOp.validTime = validTime;
	    return deleteOp;
	}
    }

    public PersistentVector getOperation() {
	operation = operation.cons(deleteId);
	if (validTime != null)
	    operation = operation.cons(validTime);
	return operation;
    }
}
