package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Binding;
import xtdb.api.query.Query;

import java.io.IOException;

public class UnnestColDeserializer extends StdDeserializer<Query.UnnestCol> {

    public UnnestColDeserializer() {
        super(Query.UnnestCol.class);
    }

    @Override
    public Query.UnnestCol deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.has("unnest")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-unnest"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode unnest = node.get("unnest");
        if (!unnest.isObject() || !(unnest.size() == 1)) {
            throw IllegalArgumentException.createNoKey("Unnest should be an object with only a single binding", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));

        }
        return Query.unnestCol(codec.treeToValue(unnest, Binding.class));
    }
}
