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

public class WithColsDeserializer extends StdDeserializer<Query.WithCols> {

    public WithColsDeserializer() {
        super(Query.WithCols.class);
    }

    @Override
    public Query.WithCols deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.has("with")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-with"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode with = node.get("with");
        if (!with.isArray()) {
            throw new IllegalArgumentException("With should be a list of bindings", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
        return Query.withCols(SpecListDeserializer.<Binding>nodeToSpecs(codec, with, Binding::new));
    }
}
