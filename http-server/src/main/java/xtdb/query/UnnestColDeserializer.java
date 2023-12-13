package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.List;

public class UnnestColDeserializer extends StdDeserializer<Query.UnnestCol> {

    public UnnestColDeserializer() {
        super(Query.UnnestCol.class);
    }

    private List<ColSpec> deserializeCols(ObjectMapper mapper, JsonNode node) throws Exception {
        return SpecListDeserializer.nodeToColSpecs(mapper, node);
    }

    @Override
    public Query.UnnestCol deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            JsonNode unnest = node.get("unnest");
            if (unnest.isObject() && unnest.size() == 1) {
                List<ColSpec> colSpecs = deserializeCols(mapper, unnest);
                return Query.unnestCol(colSpecs.get(0));
            } else {
                throw new IllegalArgumentException("Unnest should be an object with only a single binding", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
            }
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-unnest"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
