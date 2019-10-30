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
    private boolean validTimeSet = false;

    public static class Builder implements OperationBuilder {
	private PersistentVector operation;
	private Object deleteId;
	private Date validTime;
	private boolean validTimeSet = false;

	public Builder() {
	    operation = PersistentVector.create();
	    operation = operation.cons(Keyword.intern("crux.tx/delete"));
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
	    validTimeSet = true;
	    return this;
	}

	public DeleteOperation build() {
	    DeleteOperation deleteOp = new DeleteOperation();
	    deleteOp.operation = operation;
	    deleteOp.deleteId = deleteId;
	    deleteOp.validTime = validTime;
	    deleteOp.validTimeSet = validTimeSet;
	    return deleteOp;
	}
    }

    public PersistentVector getOperation() {
	operation = operation.cons(deleteId);
	if (validTimeSet)
	    operation = operation.cons(validTime);
	return operation;
    }
}
