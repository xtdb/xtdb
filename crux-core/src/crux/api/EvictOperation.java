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

    public static class Builder implements OperationBuilder {
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

	public Builder() {
	    operation = PersistentVector.create();
	    operation = operation.cons(Keyword.intern("crux.tx/evict"));
	}

	public Builder putId(String id) {
	    evictId = Keyword.intern(id);
	    return this;
	}

	public Builder putId(UUID id) {
	    evictId = id;
	    return this;
	}

	public Builder putId(URL id) {
	    evictId = id;
	    return this;
	}

	public Builder putId(URI id) {
	    evictId = id;
	    return this;
	}

	public Builder putValidTime(Date validtime) {
	    startValidTime = validtime;
	    startValidTimeSet = true;
	    return this;
	}

	public Builder putEndValidTime(Date validtime) {
	    endValidTime = validtime;
	    endValidTimeSet = true;
	    return this;
	}

	public Builder keepLatest(boolean keep) {
	    keepLatest = keep;
	    keepLatestSet = true;
	    return this;
	}

	public Builder keepEarliest(boolean keep) {
	    keepEarliest = keep;
	    keepEarliestSet = true;
	    return this;
	}

	public EvictOperation build() {
	    EvictOperation evictOp = new EvictOperation();
	    evictOp.operation = operation;
	    evictOp.evictId = evictId;
	    evictOp.startValidTime = startValidTime;
	    evictOp.startValidTimeSet = startValidTimeSet;
	    evictOp.endValidTime = endValidTime;
	    evictOp.endValidTimeSet = endValidTimeSet;
	    evictOp.keepLatest = keepLatest;
	    evictOp.keepLatestSet = keepLatestSet;
	    evictOp.keepEarliest = keepEarliest;
	    evictOp.keepEarliestSet = keepEarliestSet;
	    return evictOp;
	}
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
