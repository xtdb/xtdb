package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AggregateDeserializer extends StdDeserializer<Query.Aggregate> {

    public AggregateDeserializer() {
        super(Query.Aggregate.class);
    }

    private List<ColSpec> deserializeColSpec(ObjectMapper mapper, ArrayNode node) throws JsonProcessingException {
        List<ColSpec> res = new ArrayList<>();
        for (JsonNode argSpecNode : node) {
            res.add(mapper.treeToValue(argSpecNode, ColSpec.class));
        }
        return res;
    }
    public Query.Aggregate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        try {
            return Query.aggregate(deserializeColSpec(mapper, (ArrayNode) node.get("aggregate")));
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-aggregate"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
