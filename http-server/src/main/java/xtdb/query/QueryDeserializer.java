package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class QueryDeserializer extends StdDeserializer<Query> {


    public QueryDeserializer() {
        super(Query.class);
    }

    @Override
    public Query deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (node.isArray()) {
            return codec.treeToValue(node, Query.Pipeline.class);
        }
        if (node.isObject()) {
            if (node.has("unify")) {
                return codec.treeToValue(node, Query.Unify.class);
            }
            if (node.has("from")) {
                return codec.treeToValue(node, Query.From.class);
            }
            if (node.has("rel")) {
                return codec.treeToValue(node, Query.Relation.class);
            }
        }
        throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
    }
}
