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
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;

public class UnifyDeserializer extends StdDeserializer<Query.Unify> {

    public UnifyDeserializer() {
        super(Query.Unify.class);
    }

    @Override
    public Query.Unify deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            ObjectNode objectNode = (ObjectNode) node;
            if (objectNode.has("unify")) {
                ArrayList<Query.UnifyClause> clauses = new ArrayList<>();
                ArrayNode clauseArray = (ArrayNode) objectNode.get("unify");
                for (JsonNode clauseNode: clauseArray) {
                    clauses.add(mapper.treeToValue(clauseNode, Query.UnifyClause.class));
                }
                return Query.unify(clauses);
            } else {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-unify"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-unify"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
