package crux.kafka.nippy;

import java.util.Map;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Serializer;

public class NippySerializer implements Serializer<Object> {
    private static final IFn freeze;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy"));
        freeze = Clojure.var("juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy/fast-freeze");
    }

    public void close() {
    }

    public void configure(Map<String,?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        return (byte[]) freeze.invoke(data);
    }
}
