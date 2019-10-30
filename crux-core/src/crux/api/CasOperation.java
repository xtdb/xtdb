package crux.api;

import java.util.Map;
import java.util.HashMap;
import clojure.lang.PersistentVector;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class CasOperation implements Operation {
    private PersistentVector operation;
    private Map<Object, Object> oldMap;
    private Map<Object, Object> newMap;
    private Date validTime;
    private boolean validTimeSet = false;

    public static class Builder implements OperationBuilder {
	private PersistentVector operation;
	private Map<Object, Object> oldMap;
	private Map<Object, Object> newMap;
	private Date validTime;
	private boolean validTimeSet = false;

	public Builder() {
	    operation = PersistentVector.create();
	    operation = operation.cons(Keyword.intern("crux.tx/cas"));
	    oldMap = new HashMap<Object, Object>();
	    newMap = new HashMap<Object, Object>();
	}

	public Builder putId(String id) {
	    oldMap.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
	    newMap.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
	    return this;
	}

	public Builder putId(UUID id) {
	    oldMap.put(Keyword.intern("crux.db/id"), id);
	    newMap.put(Keyword.intern("crux.db/id"), id);
	    return this;
	}

	public Builder putId(URL id) {
	    oldMap.put(Keyword.intern("crux.db/id"), id);
	    newMap.put(Keyword.intern("crux.db/id"), id);
	    return this;
	}

	public Builder putId(URI id) {
	    oldMap.put(Keyword.intern("crux.db/id"), id);
	    newMap.put(Keyword.intern("crux.db/id"), id);
	    return this;
	}

	public Builder putValidTime(Date validtime) {
	    validTime = validtime;
	    validTimeSet = true;
	    return this;
	}

	public Builder putInOldMap(String key, Object val) {
	    oldMap.put(Keyword.intern(key), val);
	    return this;
	}

	public Builder putInOldMap(Map<Object, Object> valueMap) {
	    oldMap.putAll(valueMap);
	    return this;
	}

	public Builder putInNewMap(String key, Object val) {
	    newMap.put(Keyword.intern(key), val);
	    return this;
	}

	public Builder putInNewMap(Map<Object,Object> valueMap) {
	    newMap.putAll(valueMap);
	    return this;
	}

	public CasOperation build() {
	    CasOperation casOp = new CasOperation();
	    casOp.operation = operation;
	    casOp.oldMap = oldMap;
	    casOp.newMap = newMap;
	    casOp.validTime = validTime;
	    casOp.validTimeSet = validTimeSet;
	    return casOp;
	}
    }
    public PersistentVector getOperation() {
	operation = operation.cons(oldMap);
	operation = operation.cons(newMap);
	if (validTimeSet)
	    operation = operation.cons(validTime);
	return operation;
    }
}
