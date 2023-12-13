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

public class WithDeserializer extends StdDeserializer<Query.With> {

    public WithDeserializer() {
        super(Query.With.class);
    }

    @Override
    public Query.With deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);
        try {
            JsonNode with = node.get("with");

            if (with.isArray()) {
                List<VarSpec> vars = new ArrayList<>();

                for (JsonNode varNode : with) {
                    VarSpec varSpec = mapper.treeToValue(varNode, VarSpec.class);
                    vars.add(varSpec);
                }
        
                return Query.with(vars);
            } else {
                throw new Exception("With should be a list of bindings");
            }
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-with"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}