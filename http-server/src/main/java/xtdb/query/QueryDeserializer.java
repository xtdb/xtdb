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
        JsonNode node = mapper.readTree(p);

        if (node.isArray()) {
            return mapper.treeToValue(node, Query.Pipeline.class);
        }
        if (node.has("unify")) {
            return mapper.treeToValue(node, Query.Unify.class);
        }
        if (node.has("from")) {
            return mapper.treeToValue(node, Query.From.class);
        }
        if (node.has("rel")) {
            return mapper.treeToValue(node, Query.Relation.class);
        }
        // TODO everything else
        throw new IllegalArgumentException("unsupported", PersistentHashMap.EMPTY, null);
    }
}
