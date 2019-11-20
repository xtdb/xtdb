package crux.api.alpha;

import java.net.URI;
import java.net.URL;
import java.util.UUID;

import static crux.api.alpha.Util.keyword;

public class CruxId {
    private final Object id;

    private CruxId(Object id) {
        this.id = id;
    }

    public static CruxId cruxId(String name) {
        return new CruxId(keyword(name));
    }

    public static CruxId cruxId(UUID uuid) {
        return new CruxId(uuid);
    }

    public static CruxId cruxId(URI uri) {
        return new CruxId(uri);
    }

    public static CruxId cruxId(URL url) {
        return new CruxId(url);
    }

    public static CruxId cruxId(Object obj) {
        return new CruxId(obj);
    }

    protected Object toEdn() {
        return id;
    }
}
