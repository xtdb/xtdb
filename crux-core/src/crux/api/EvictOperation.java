package crux.api;

import java.util.List;
import java.util.ArrayList;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class EvictOperation implements Operation {
    public static class Builder implements OperationBuilder {
	private List<Object> operation;
	private Object evictId;
	private Date startValidTime;
	private Date endValidTime;
	private Boolean keepLatest;
	private Boolean keepEarliest;

	private void init() {
	    operation = new ArrayList<Object>();
	    operation.add(Keyword.intern("crux.tx/evict"));
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

	public List<Object> build() {
	    EvictOperation evictOp = new EvictOperation();
	    operation.add(evictId);
	    if (startValidTime != null)
		operation.add(startValidTime);
	    if (endValidTime != null)
	        operation.add(endValidTime);
	    if (keepLatest != null)
		operation.add(keepLatest);
	    if (keepEarliest != null)
		operation.add(keepEarliest);
	    return operation;
	}
    }
}
