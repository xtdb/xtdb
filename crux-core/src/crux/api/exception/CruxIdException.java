package crux.api.exception;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;

import java.net.URI;
import java.net.URL;
import java.util.UUID;

public class CruxIdException extends RuntimeException {
    private static final long serialVersionUID = 908571272;

    public static void assertValidType(Object o) {
        if (!(o instanceof String)
            && !(o instanceof Keyword)
            && !(o instanceof Integer)
            && !(o instanceof Long)
            && !(o instanceof UUID)
            && !(o instanceof URI)
            && !(o instanceof URL)
            && !(o instanceof IPersistentMap)) {
            if (o == null) {
                throw new CruxIdException(null);
            }
            else {
                throw new CruxIdException(o.getClass().toString());
            }
        }
    }

    private CruxIdException(String className) {
        super(className + " is not a valid CruxId type");
    }
}
