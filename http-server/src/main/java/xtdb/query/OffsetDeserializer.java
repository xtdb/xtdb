package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Query;

import java.io.IOException;

public class OffsetDeserializer extends StdDeserializer<Query.Offset> {
    public OffsetDeserializer() {
        super(Query.Offset.class);
    }

    @Override
    public Query.Offset deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        var codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.isObject() || !node.has("offset")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-offset"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode offsetNode = node.get("offset");
        if (!(offsetNode.isLong() || offsetNode.isInt())) {
            throw new IllegalArgumentException("Offset should be a valid number", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
        long offset = offsetNode.asLong(); // Parse as long
        return Query.offset(offset);
    }
}
