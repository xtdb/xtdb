package crux.kafka.json;

import java.util.Map;
import java.io.UnsupportedEncodingException;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer implements Deserializer<Object> {
    private static final IFn parseString;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("juxt.clojars-mirrors.cheshire.v5v10v0.cheshire.core"));
        parseString = Clojure.var("juxt.clojars-mirrors.cheshire.v5v10v0.cheshire.core/parse-string");
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
            return parseString.invoke(new String(data, "UTF-8"), true);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
