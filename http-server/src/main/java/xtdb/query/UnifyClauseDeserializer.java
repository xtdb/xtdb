package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;

public class UnifyClauseDeserializer extends StdDeserializer<Query.UnifyClause> {

    public UnifyClauseDeserializer() {
        super(Query.UnifyClause.class);
    }

    public Query.UnifyClause deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        if (node.has("from")) {
            return mapper.treeToValue(node, Query.From.class);
        } else {
            // TODO join, left-join, rel, unnest, where, with
            throw new IllegalArgumentException("unsupported", PersistentHashMap.EMPTY, null);
        }
    }
}
