package crux.kafka.edn;

import java.util.Map;
import java.io.UnsupportedEncodingException;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Serializer;

public class EdnSerializer implements Serializer<Object> {
    private static final IFn prStr;
     private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    private static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    static {
        prStr = resolve("crux.io/pr-edn-str");
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
