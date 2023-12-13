package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class QueryTailDeserializer extends StdDeserializer<Query.QueryTail> {
    public QueryTailDeserializer() {
        super(Query.QueryTail.class);
    }

    @Override
    public Query.QueryTail deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.has("where")) {
                return mapper.treeToValue(node, Query.Where.class);
            }
            if (node.has("limit")) {
                return mapper.treeToValue(node, Query.Limit.class);
            }
            if (node.has("offset")) {
                return mapper.treeToValue(node, Query.Offset.class);
            }
            if (node.has("orderBy")) {
                return mapper.treeToValue(node, Query.OrderBy.class);
            }
            if (node.has("return")) {
                return mapper.treeToValue(node, Query.Return.class);
            }
            if (node.has("unnest")) {
                return mapper.treeToValue(node, Query.UnnestCol.class);
            }
            if (node.has("with")) {
                return mapper.treeToValue(node, Query.WithCols.class);
            }
            if (node.has("without")) {
                return mapper.treeToValue(node, Query.Without.class);
            }
            if (node.has("aggregate")) {
                return mapper.treeToValue(node, Query.Aggregate.class);
            }
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-tail"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-tail"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}