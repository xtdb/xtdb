package xtdb.kafka.transit;

import java.util.Map;
import java.util.HashMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import clojure.java.api.Clojure;
import clojure.lang.AFn;
import clojure.lang.IDeref;
import clojure.lang.IFn;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.core.JsonGenerator;

public class TransitSerializer implements Serializer<Object> {
    private static final IFn write;
    private static final IFn writer;
    private static final IFn writeHandler;
    private static final IFn ednIdToOriginalId;
    private static final IFn clojureStr;
    private static final Object jsonVerbose;
    private static final Map<Object, Object> options;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("cognitect.transit"));
        write = Clojure.var("cognitect.transit/write");
        writer = Clojure.var("cognitect.transit/writer");
        writeHandler = Clojure.var("cognitect.transit/write-handler");
        Clojure.var("clojure.core/require").invoke(Clojure.read("xtdb.codec"));
        ednIdToOriginalId = (IFn) ((IDeref) Clojure.var("xtdb.codec/edn-id->original-id")).deref();
        clojureStr = (IFn) ((IDeref) Clojure.var("clojure.core/str")).deref();
        jsonVerbose = Clojure.read(":json-verbose");
        Map<Object, Object> handlers = new HashMap<>();
        handlers.put(Clojure.var("clojure.core/resolve").invoke(Clojure.read("xtdb.codec.EDNId")),
                     writeHandler.invoke("xt/id", ednIdToOriginalId));
        handlers.put(Clojure.var("clojure.core/resolve").invoke(Clojure.read("xtdb.codec.Id")),
                     writeHandler.invoke("xt/id", clojureStr));
        options = new HashMap<>();
        options.put(Clojure.read(":handlers"), handlers);
    }

    public void close() {
    }

    public void configure(Map<String,?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            write.invoke(writer.invoke(out, jsonVerbose, options), data);
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
