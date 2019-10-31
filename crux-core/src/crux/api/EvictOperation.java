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
    private Date endValidTime;
    private Boolean keepLatest;
    private Boolean keepEarliest;

    public static class Builder implements OperationBuilder {
	private PersistentVector operation;
	private Object evictId;
	private Date startValidTime;
	private Date endValidTime;
	private Boolean keepLatest;
	private Boolean keepEarliest;

	private void init() {
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

	public Builder putValidTime(Date validtime) {
	    startValidTime = validtime;
	    return this;
	}

	public Builder putEndValidTime(Date validtime) {
	    endValidTime = validtime;
	    return this;
	}

	public Builder keepLatest(boolean keep) {
	    keepLatest = keep;
	    return this;
	}

	public Builder keepEarliest(boolean keep) {
	    keepEarliest = keep;
	    return this;
	}

	public EvictOperation build() {
	    EvictOperation evictOp = new EvictOperation();
	    evictOp.operation = operation;
	    evictOp.evictId = evictId;
	    evictOp.startValidTime = startValidTime;
	    evictOp.endValidTime = endValidTime;
	    evictOp.keepLatest = keepLatest;
	    evictOp.keepEarliest = keepEarliest;
	    return evictOp;
	}
    }

    public PersistentVector getOperation() {
	operation = operation.cons(evictId);
	if (startValidTime != null)
	    operation = operation.cons(startValidTime);
	if (endValidTime != null)
	    operation = operation.cons(endValidTime);
	if (keepLatest != null)
	    operation = operation.cons(keepLatest);
	if (keepEarliest != null)
	    operation = operation.cons(keepEarliest);
	return operation;
    }
}
