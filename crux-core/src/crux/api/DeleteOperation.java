package crux.api;

import java.util.List;
import java.util.ArrayList;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class DeleteOperation implements Operation {
    public static class Builder implements OperationBuilder {
	private List<Object> operation;
	private Object deleteId;
	private Date validTime;

	private void init() {
	    operation = new ArrayList<Object>();
	    operation.add(Keyword.intern("crux.tx/delete"));
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

	public List<Object> build() {
	    DeleteOperation deleteOp = new DeleteOperation();
	    operation.add(deleteId);
	    if (validTime != null)
		operation.add(validTime);
	    return operation;
	}
    }

}
