package crux.api;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import clojure.lang.Keyword;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public class CasOperation implements Operation {
    public static class Builder implements OperationBuilder {
	private List<Object> operation;
	private Map<Keyword, Object> oldMap;
	private Map<Keyword, Object> newMap;
	private Date validTime;

	private void init() {
	    operation = new ArrayList<Object>();
	    operation.add(Keyword.intern("crux.tx/cas"));
	    oldMap = new HashMap<Keyword, Object>();
	    newMap = new HashMap<Keyword, Object>();
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
	    return this;
	}

	public Builder putInOldMap(String key, Object val) {
	    oldMap.put(Keyword.intern(key), val);
	    return this;
	}

	public Builder putInOldMap(Map<String, Object> valueMap) {
	    for (String key : valueMap.keySet()) {
		oldMap.put(Keyword.intern(key), valueMap.get(key));
	    }
	    return this;
	}

	public Builder putInNewMap(String key, Object val) {
	    newMap.put(Keyword.intern(key), val);
	    return this;
	}

	public Builder putInNewMap(Map<String,Object> valueMap) {
	    for (String key : valueMap.keySet()) {
		newMap.put(Keyword.intern(key), valueMap.get(key));
	    }
	    return this;
	}

	public List<Object> build() {
	    CasOperation casOp = new CasOperation();
	    operation.add(oldMap);
	    operation.add(newMap);
	    if (validTime != null)
		operation.add(validTime);
	    return operation;
	}
    }
}
