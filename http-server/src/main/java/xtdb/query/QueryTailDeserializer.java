package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class QueryTailDeserializer extends StdDeserializer<Query.QueryTail> {
    public QueryTailDeserializer() {
        super(Query.QueryTail.class);
    }

    @Override
    public Query.QueryTail deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        if (node.has("limit")) {
            return mapper.treeToValue(node, Query.Limit.class);
        } else if (node.has("offset")) {
            return mapper.treeToValue(node, Query.Offset.class);
        } else {
            // TODO everything else
            throw new IllegalArgumentException("unsupported", PersistentHashMap.EMPTY, null);
        }
    }
}
