package xtdb.kafka.edn;

import java.util.Map;
import java.io.UnsupportedEncodingException;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Deserializer;

public class EdnDeserializer implements Deserializer<Object> {
    private static final IFn readString;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.codec"));
        readString = Clojure.var("crux.codec/read-edn-string-with-readers");
    }

    public void close() {
    }

    public void configure(Map<String,?> configs, boolean isKey) {
    }

    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return readString.invoke(new String(data, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
