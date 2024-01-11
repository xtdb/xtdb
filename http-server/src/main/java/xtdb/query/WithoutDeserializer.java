package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WithoutDeserializer extends StdDeserializer<Query.Without> {

    public WithoutDeserializer() {
        super(Query.Without.class);
    }

    @Override
    public Query.Without deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.has("without")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-without"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode without = node.get("without");
        if (!without.isArray()) {
            throw IllegalArgumentException.createNoKey("Without should be a list of strings", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        List<String> cols = new ArrayList<>();
        for (JsonNode colNode : without) {
            if (!colNode.isTextual()) {
                throw IllegalArgumentException.createNoKey("All items in Without clause must be strings", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
            }
            cols.add(colNode.asText());
        }
        return Query.without(cols);
    }
}
