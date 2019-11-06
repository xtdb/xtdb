package crux.api;

import java.util.Map;
import java.util.HashMap;
import clojure.lang.PersistentVector;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class PutOperation implements Operation {
    private PersistentVector operation;
    private Map<Keyword, Object> query;
    private Date validTime;

    public static class Builder implements OperationBuilder {
	private PersistentVector operation;
	private Map<Keyword, Object> query;
	private Date validTime;

	private void init() {
	    operation = PersistentVector.create();
	    operation = operation.cons(Keyword.intern("crux.tx/put"));
	    query = new HashMap<Keyword, Object>();
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
	    query.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
	    return this;
	}

	public Builder putId(UUID id) {
	    query.put(Keyword.intern("crux.db/id"), id);
	    return this;
	}

	public Builder putId(URL id) {
	    query.put(Keyword.intern("crux.db/id"), id);
	    return this;
	}

	public Builder putId(URI id) {
	    query.put(Keyword.intern("crux.db/id"), id);
	    return this;
	}

	public Builder putValidTime(Date validtime) {
	    validTime = validtime;
	    return this;
	}

	public Builder put(String key, Object val) {
	    query.put(Keyword.intern(key), val);
	    return this;
	}

	public Builder put(Map<String,Object> valueMap) {
	    for (String key : valueMap.keySet()) {
		query.put(Keyword.intern(key), valueMap.get(key));
	    }
	    return this;
	}

	public PutOperation build() {
	    PutOperation putOp = new PutOperation();
	    putOp.operation = operation;
	    putOp.query = query;
	    putOp.validTime = validTime;
	    return putOp;
	}
    }
    public PersistentVector getOperation() {
	operation = operation.cons(query);
	if (validTime != null)
	    operation = operation.cons(validTime);
	return operation;
    }
}
