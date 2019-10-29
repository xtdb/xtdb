package crux.api;

import clojure.lang.PersistentVector;
import java.util.Date;
import java.util.UUID;
import java.net.URI;
import java.net.URL;

public interface Operation {
    public void putValidTime(Date validTime);

    public PersistentVector getOperation();
}
