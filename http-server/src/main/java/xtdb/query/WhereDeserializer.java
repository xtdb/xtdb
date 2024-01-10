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
import xtdb.api.query.Expr;
import xtdb.api.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WhereDeserializer extends StdDeserializer<Query.Where> {

    public WhereDeserializer() {
        super(Query.Where.class);
    }

    @Override
    public Query.Where deserialize(JsonParser p, DeserializationContext ctxt) 
            throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.has("where")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-where"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        JsonNode where = node.get("where");
        if (!where.isArray()) {
            throw new IllegalArgumentException("Where should be a list of expressions", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
        List<Expr> predicates = new ArrayList<>();
        for (JsonNode predicateNode : where) {
            Expr predicate = codec.treeToValue(predicateNode, Expr.class);
            predicates.add(predicate);
        }
        return Query.where(predicates);
    }
}
