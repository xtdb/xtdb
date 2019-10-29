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

    public void putId(Map query, String id) {
	query.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
    }

    public void putId(Map query, UUID id) {
	query.put(Keyword.intern("crux.db/id"), id);
    }

    public void putId(Map query, URL id) {
	query.put(Keyword.intern("crux.db/id"), id);
    }

    public void putId(Map query, URI id) {
	query.put(Keyword.intern("crux.db/id"), id);
    }

    public void putOldMapId(Object id) {
	putId(oldMap, id);
    }

    public void putNewMapId(Object id) {
	putId(newMap, id);
    }

    public void putValidTime(Date validtime) {
	validTime = validtime;
	validTimeSet = true;
    }

    public void putOldMap(Object key, Object val) {
	oldMap.put(key, val);
    }

    public void putNewMap(Object key, Object val) {
	newMap.put(key, val);
    }

    public PersistentVector getOperation() {
	operation = operation.cons(oldMap);
	operation = operation.cons(newMap);
	if (validTimeSet)
	    operation = operation.cons(validTime);
	return operation;
    }
}
