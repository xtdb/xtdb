package crux.api.document;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import crux.api.exception.CruxIdException;

import java.net.URI;
import java.net.URL;
import java.util.Objects;
import java.util.UUID;

public class CruxId {
    public static Object validate(Object o) {
        if (o == null) {
            throw new CruxIdException(null);
        }

        if (!(o instanceof String)
                && !(o instanceof Keyword)
                && !(o instanceof Integer)
                && !(o instanceof Long)
                && !(o instanceof UUID)
                && !(o instanceof URI)
                && !(o instanceof URL)
                && !(o instanceof IPersistentMap)
                && !isSpecial(o)) {
            throw new CruxIdException(o.getClass().toString());
        }

        return o;
    }

    //TODO: Figure out how to remove these
    public static boolean isSpecial(Object o) {
        return o.getClass().getName().equals("crux.codec.Id");
    }
    public static boolean equals(Object o1, Object o2) {
        if (isSpecial(o1) || isSpecial(o2)) {
            return true;
        }

        return Objects.equals(o1, o2);
    }
}
