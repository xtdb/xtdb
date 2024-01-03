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

public class UnnestVarDeserializer extends StdDeserializer<Query.UnnestVar> {

    public UnnestVarDeserializer() {
        super(Query.UnnestVar.class);
    }

    @Override
    public Query.UnnestVar deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec mapper = p.getCodec();
        JsonNode node = mapper.readTree(p);

        if (!node.has("unnest")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-unnest"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode unnest = node.get("unnest");
        if (!unnest.isObject() || !(unnest.size() == 1)) {
            throw new IllegalArgumentException("Unnest should be an object with only a single binding", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
        return Query.unnestVar(mapper.treeToValue(unnest, Binding.class));
    }
}
