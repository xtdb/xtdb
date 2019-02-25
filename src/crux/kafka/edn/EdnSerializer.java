package crux.kafka.edn;

import java.util.Map;
import java.io.UnsupportedEncodingException;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Serializer;

public class EdnSerializer implements Serializer<Object> {
    private static final IFn prStr;

    static {
        prStr = Clojure.var("clojure.core/pr-str");
    }

    public void close() {
    }

    public void configure(Map<String,?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try {
            return ((String) prStr.invoke(data)).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
