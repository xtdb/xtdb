package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Query;

import java.io.IOException;

public class UnifyClauseDeserializer extends StdDeserializer<Query.UnifyClause> {

    public UnifyClauseDeserializer() {
        super(Query.UnifyClause.class);
    }

    public Query.UnifyClause deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        ObjectNode node = codec.readTree(p);

        if (node.has("from")) {
            return codec.treeToValue(node, Query.From.class);
        }
        if (node.has("where")) {
            return codec.treeToValue(node, Query.Where.class);
        }
        if (node.has("unnest")) {
            return codec.treeToValue(node, Query.UnnestVar.class);
        }
        if (node.has("with")) {
            return codec.treeToValue(node, Query.With.class);
        }
        if (node.has("join") || node.has("left_join")) {
            return codec.treeToValue(node, Query.IJoin.class);
        }
        if (node.has("rel")) {
            return codec.treeToValue(node, Query.Relation.class);
        }
        throw IllegalArgumentException.create(Keyword.intern("xtql", "unsupported-unify-clause"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
    }
}
