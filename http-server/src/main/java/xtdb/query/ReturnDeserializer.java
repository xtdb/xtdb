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

public class ReturnDeserializer extends StdDeserializer<Query.Return> {

    public ReturnDeserializer() {
        super(Query.Return.class);
    }

    @Override
    public Query.Return deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.isObject() || !node.has("return")){
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-return"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode returnNode = node.get("return");
        if (returnNode.isArray()) {
            return Query.returning(SpecListDeserializer.nodeToSpecs(codec, returnNode, Binding::new));
        } else {
            throw new IllegalArgumentException("Return should be a list of values", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
    }
}
