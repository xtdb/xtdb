package xtdb.kafka.nippy;

import java.util.Map;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Deserializer;

public class NippyDeserializer implements Deserializer<Object> {
    private static final IFn thaw;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy"));
        thaw = Clojure.var("juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy/fast-thaw");
    }

    public void close() {
    }

    public void configure(Map<String,?> configs, boolean isKey) {
    }

    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return thaw.invoke(data);
    }
}
