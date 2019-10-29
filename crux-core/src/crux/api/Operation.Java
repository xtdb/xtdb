package crux.api;

import clojure.java.api.Clojure;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public interface Operation {
    public void putValidTime(Date validTime);

    public PersistentVector getOperation();
}
