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

public class OrderByDeserializer extends StdDeserializer<Query.OrderBy> {

    public OrderByDeserializer() {
        super(Query.OrderBy.class);
    }

    private Query.OrderDirection parseDirection(JsonNode node) throws IllegalArgumentException {
        if(node == null) return null;
        String dir = node.asText();
        if (dir == null) return null;
        switch (dir) {
            case "asc":
                return Query.OrderDirection.ASC;
            case "desc":
                return Query.OrderDirection.DESC;
            default:
                throw new IllegalArgumentException("Invalid orderBy direction, must be one of ['asc', 'desc']", PersistentHashMap.create(Keyword.intern("dir"), dir), null);
        }
    }

    private Query.OrderNulls parseNulls(JsonNode node) throws IllegalArgumentException{
        if (node == null) return null;
        String nulls  = node.asText();
        if (nulls == null) return null;
        switch (nulls) {
            case "first":
                return Query.OrderNulls.FIRST;
            case "last":
                return Query.OrderNulls.LAST;
            default:
                throw new IllegalArgumentException("Invalid orderBy nulls, must be one of ['first', 'last']", PersistentHashMap.create(Keyword.intern("nulls"), nulls), null);
        }
    }

    private Query.OrderSpec parseOrderSpec(ObjectCodec codec, JsonNode orderSpecNode) throws IllegalArgumentException, JsonProcessingException {
        if (orderSpecNode.isTextual()) {
            String var = orderSpecNode.asText();
            Expr varExpr = Expr.lVar(var);
            return Query.orderSpec(varExpr, null, null);
        } else {
            Expr valExpr = codec.treeToValue(orderSpecNode.get("val"), Expr.class);
            Query.OrderDirection direction = parseDirection(orderSpecNode.get("dir"));
            Query.OrderNulls nulls = parseNulls(orderSpecNode.get("nulls"));
            return Query.orderSpec(valExpr, direction, nulls);
        }
    }

    @Override
    public Query.OrderBy deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, IllegalArgumentException{
        ObjectCodec codec =  p.getCodec();
        JsonNode node = codec.readTree(p);
        JsonNode orderBy = node.get("orderBy");

        if (!orderBy.isArray()) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-order-by"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        List<Query.OrderSpec> orderSpecs = new ArrayList<>();
        for (JsonNode orderSpecNode : orderBy) {
            orderSpecs.add(parseOrderSpec(codec, orderSpecNode));
        }
        return Query.orderBy(orderSpecs);
    }
}
