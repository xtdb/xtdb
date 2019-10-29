package crux.api;

import clojure.lang.PersistentVector;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class EvictOperation implements Operation {
    private PersistentVector operation;
    private Object evictId;
    private Date startValidTime;
    private boolean startValidTimeSet = false;
    private Date endValidTime;
    private boolean endValidTimeSet = false;
    private boolean keepLatest;
    private boolean keepLatestSet = false;
    private boolean keepEarliest;
    private boolean keepEarliestSet = false;

    public EvictOperation() {
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
	startValidTimeSet = true;
    }

    public void putEndValidTime(Date validtime) {
	endValidTime = validtime;
	endValidTimeSet = true;
    }

    public void keepLatest(boolean keep) {
	keepLatest = keep;
	keepLatestSet = true;
    }

    public void keepEarliest(boolean keep) {
	keepEarliest = keep;
	keepEarliestSet = true;
    }

    public PersistentVector getOperation() {
	operation = operation.cons(evictId);
	if (startValidTimeSet)
	    operation = operation.cons(startValidTime);
	if (endValidTimeSet)
	    operation = operation.cons(endValidTime);
	if (keepLatestSet)
	    operation = operation.cons(keepLatest);
	if (keepEarliestSet)
	    operation = operation.cons(keepEarliest);
	return operation;
    }
}
