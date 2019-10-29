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

    public void putOldMapId(String id) {
	oldMap.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
    }

    public void putOldMapId(UUID id) {
	oldMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putOldMapId(URL id) {
	oldMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putOldMapId(URI id) {
	oldMap.put(Keyword.intern("crux.db/id"), id);
    }

     public void putNewMapId(String id) {
	newMap.put(Keyword.intern("crux.db/id"), Keyword.intern(id));
    }

    public void putNewMapId(UUID id) {
	newMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putNewMapId(URL id) {
	newMap.put(Keyword.intern("crux.db/id"), id);
    }

    public void putNewMapId(URI id) {
	newMap.put(Keyword.intern("crux.db/id"), id);
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
