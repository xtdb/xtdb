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

    public CasOperation() {
	operation = PersistentVector.create();
	operation = operation.cons(Keyword.intern("crux.tx/cas"));
	oldMap = new HashMap<Object, Object>();
	newMap = new HashMap<Object, Object>();
    }

    public void putId(String id) {
	oldMap.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
	newMap.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
    }

    public void putId(UUID id) {
	oldMap.put(Keyword.intern("crux.db/id"), id);
	newMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putId(URL id) {
	oldMap.put(Keyword.intern("crux.db/id"), id);
	newMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putId(URI id) {
	oldMap.put(Keyword.intern("crux.db/id"), id);
	newMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putValidTime(Date validtime) {
	validTime = validtime;
	validTimeSet = true;
    }

    public void putInOldMap(Object key, Object val) {
	oldMap.put(key, val);
    }

    public void putInOldMapAtId(String key, Object val) {
	putInOldMap(Keyword.intern(key), val);
    }

    public void putInNewMap(Object key, Object val) {
	newMap.put(key, val);
    }

    public void putInNewMapAtId(String key, Object val) {
	putInNewMap(Keyword.intern(key), val);
    }

    public PersistentVector getOperation() {
	operation = operation.cons(oldMap);
	operation = operation.cons(newMap);
	if (validTimeSet)
	    operation = operation.cons(validTime);
	return operation;
    }
}
