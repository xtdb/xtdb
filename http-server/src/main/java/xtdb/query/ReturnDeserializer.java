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
import java.util.ArrayList;
import java.util.List;

public class ReturnDeserializer extends StdDeserializer<Query.Return> {

    public ReturnDeserializer() {
        super(Query.Return.class);
    }

    private List<ColSpec> deserializeCols(ObjectMapper mapper, JsonNode node) throws Exception {
        return SpecListDeserializer.nodeToColSpecs(mapper, node);
    }

    @Override
    public Query.Return deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);
        try {
            JsonNode returnNode = node.get("return");

            if (returnNode.isArray()) {
                return Query.returning(deserializeCols(mapper, returnNode));
            } else {
                throw new IllegalArgumentException("Return should be a list of values", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
            }
        } catch (IllegalArgumentException i) {
           throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-return"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}