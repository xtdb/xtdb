package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrderByDeserializer extends StdDeserializer<Query.OrderBy> {

    public OrderByDeserializer() {
        super(Query.OrderBy.class);
    }

    private Query.OrderDirection parseDirection(String dir) throws Exception {
        if (dir == null) return null;
        switch (dir) {
            case "asc":
                return Query.OrderDirection.ASC;
            case "desc":
                return Query.OrderDirection.DESC;
            default:
                throw new Exception("Invalid orderBy direction, must be one of ['asc', 'desc']");
        }
    }

    private Query.OrderNulls parseNulls(String nulls) throws Exception {
        if (nulls == null) return null;
        switch (nulls) {
            case "first":
                return Query.OrderNulls.FIRST;
            case "last":
                return Query.OrderNulls.LAST;
            default:
                throw new Exception("Invalid orderBy nulls, must be one of ['first', 'last']");
        }
    }

    private Query.OrderSpec parseOrderSpec(JsonNode orderSpecNode) throws Exception {
        if (orderSpecNode.isTextual()) {
            String var = orderSpecNode.asText();
            // TODO: Very basic parsing to Expr - replace when we have proper Expr deserializer
            Expr varExpr = Expr.lVar(var);
            return Query.orderSpec(varExpr, null, null);
        } else {
            String var = orderSpecNode.get("val").asText();
            // TODO: Very basic parsing to Expr - replace when we have proper Expr deserializer
            Expr varExpr = Expr.lVar(var);
            Query.OrderDirection direction = parseDirection(orderSpecNode.get("dir").asText());
            Query.OrderNulls nulls = parseNulls(orderSpecNode.get("nulls").asText());
            return Query.orderSpec(varExpr, direction, nulls);
        }
    }

    @Override
    public Query.OrderBy deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);
        JsonNode orderBy = node.get("orderBy");

        if (!orderBy.isArray()) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-order-by"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        try {
            List<Query.OrderSpec> orderSpecs = new ArrayList<>();
            for (JsonNode orderSpecNode : orderBy) {
                orderSpecs.add(parseOrderSpec(orderSpecNode));
            }
            return Query.orderBy(orderSpecs);
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-order-by"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}