package crux.kafka.transit;

import java.util.Map;
import java.util.HashMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import clojure.java.api.Clojure;
import clojure.lang.AFn;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Deserializer;

public class TransitDeserializer implements Deserializer<Object> {
    private static final IFn read;
    private static final IFn reader;
    private static final Object jsonVerbose;
    private static final Map<Object, Object> options;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("cognitect.transit"));
        read = Clojure.var("cognitect.transit/read");
        reader = Clojure.var("cognitect.transit/reader");
        IFn readHandler = Clojure.var("cognitect.transit/read-handler");
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.codec"));
        IFn idEdnReader = Clojure.var("crux.codec/id-edn-reader");
        jsonVerbose = Clojure.read(":json-verbose");
        Map<Object, Object> handlers = new HashMap<>();
        handlers.put("crux/id", readHandler.invoke(idEdnReader));
        options = new HashMap<>();
        options.put(Clojure.read(":handlers"), handlers);
    }

    public void close() {
    }

    public void configure(Map<String,?> configs, boolean isKey) {
    }

    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayInputStream in = new ByteArrayInputStream(data)){
            return read.invoke(reader.invoke(in, jsonVerbose, options));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
