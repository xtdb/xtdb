package crux.api;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class EvictOperation implements Operation {
    private PersistentVector operation;
    private Object evictId;
    private Date startValidTime = null;
    private Date endValidTime = null;
    private Boolean keepLatest = null;
    private Boolean keepEarliest = null;

    public DeleteOperation() {
	operation = PersistentVector.create();
	operation = operation.cons(Keyword.intern("crux.tx/evict"));
    }

    public void putId(String id) {
	evictId = Keyword.intern(id);
    }

    public void putId(UUID id) {
	evictId = id;
    }

    public void putId(URL id) {
	evictId = id;
    }

    public void putId(URI id) {
	evictId = id;
    }

    public void putValidTime(Date validtime) {
	startValidTime = validtime;
    }

    public void putEndValidTime(Date validtime) {
	endValidTime = validtime;
    }

    public void keepLatest(boolean keep) {
	keepLatest = keep;
    }

    public void keepLatest(boolean keep) {
	keepEarliest = keep;
    }

    public PersistentVector getOperation() {
	operation = operation.cons(evictId);
	if (startValidTime)
	    operation = operation.cons(startValidTime);
	if (endValidTime)
	    operation = operation.cons(endValidTime);
	if (keepLatest)
	    operation = operation.cons(keepLatest);
	if (keepEarliest)
	    operation = operation.cons(keepEarliest);
	return operation;
    }
}
