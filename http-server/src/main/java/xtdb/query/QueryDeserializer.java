package xtdb.query;

import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class QueryDeserializer extends StdDeserializer<Query> {


    public QueryDeserializer() {
        super(Query.class);
    }

    @Override
    public Query deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        // TODO pipeline
        if (node.isArray()) {
            throw new IllegalArgumentException("unsupported", PersistentHashMap.EMPTY, null);
        } else if (node.has("from")) {
            return mapper.treeToValue(node, Query.From.class);
        } else {
            // TODO everything else
            throw new IllegalArgumentException("unsupported", PersistentHashMap.EMPTY, null);
        }
    }

}
