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
    private Map<Object, Object> query;
    private Date validTime;
    private boolean validTimeSet = false;

    public PutOperation() {
	operation = PersistentVector.create();
	operation = operation.cons(Keyword.intern("crux.tx/put"));
	query = new HashMap<Object, Object>();
    }

    public void putId(String id) {
	query.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
    }

    public void putId(UUID id) {
	query.put(Keyword.intern("crux.db/id"), id);
    }

    public void putId(URL id) {
	query.put(Keyword.intern("crux.db/id"), id);
    }

    public void putId(URI id) {
	query.put(Keyword.intern("crux.db/id"), id);
    }

    public void putValidTime(Date validtime) {
	validTime = validtime;
	validTimeSet = true;
    }

    public void put(String key, Object val) {
	put(Keyword.intern(key), val);
    }

    public PersistentVector getOperation() {
	operation = operation.cons(query);
	if (validTimeSet)
	    operation = operation.cons(validTime);
	return operation;
    }
}
