package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class LimitDeserializer extends StdDeserializer<Query.Limit> {
    public LimitDeserializer() {
        super(Query.Limit.class);
    }

    @Override
    public Query.Limit deserialize (JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.isObject() || !node.has("limit")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-limit"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode limitNode = node.get("limit");
        if (!(limitNode.isLong() || limitNode.isInt())) {
            throw new IllegalArgumentException("Limit should be a valid number", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
        long limit = limitNode.asLong(); // Parse as long
        return Query.limit(limit);
    }
}
