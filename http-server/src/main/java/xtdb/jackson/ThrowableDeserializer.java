package xtdb.jackson;

import clojure.lang.ExceptionInfo;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.RuntimeException;

import java.io.IOException;
import java.security.Key;
import java.util.Collections;
import java.util.Map;

class ThrowableDeserializer extends JsonDeserializer<Throwable> {
    @Override
    public Throwable deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec codec = p.getCodec();
        var tree = p.readValueAsTree();

        if (!tree.isObject()) throw new java.lang.IllegalArgumentException("Expected exception to be a map");

        var messageNode = tree.get("xtdb.error/message");
        String message = messageNode != null ? codec.treeToValue(messageNode, String.class) : null;

        var dataNode = tree.get("xtdb.error/data");
        var data = dataNode != null ? codec.treeToValue(dataNode, Map.class) : Collections.emptyMap();

        var errorKeyNode = tree.get("xtdb.error/error-key");
        var errorKey = errorKeyNode != null ? codec.treeToValue(errorKeyNode, Keyword.class) : null;

        return switch (codec.treeToValue(tree.get("xtdb.error/class"), String.class)) {
            case "xtdb.IllegalArgumentException" -> new IllegalArgumentException(errorKey, message, data, null);
            case "xtdb.RuntimeException" -> new RuntimeException(errorKey, message, data, null);
            default -> new ExceptionInfo(message, PersistentHashMap.create(data));
        };

    }
}
