package crux.api;

import java.util.Map;
import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class PutOperation implements Operation{
    private PersistentVector operation;
    private Map query;
    private Date validTime = null;

    public PutOperation() {
	operation = PersistentVector.create();
	operation.cons(Keyword.intern("crux.tx/put"));
	query = new HashMap();
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
    }

    public void put(Object key, Object val) {
	query.put(key, val);
    }

    public PersistentVector getOperation() {
	operation.cons(q1);
	if (validTime)
	    operation.cons(validTime);
	return operation;
    }
}
