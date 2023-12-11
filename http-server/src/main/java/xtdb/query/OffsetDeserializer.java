package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class OffsetDeserializer extends StdDeserializer<Query.Offset> {
    public OffsetDeserializer() {
        super(Query.Offset.class);
    }

    @Override
    public Query.Offset deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            JsonNode offset = node.get("offset");
            long offsetValue = offset.asLong();
            return Query.offset(offsetValue);
        }
        catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-offset"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
