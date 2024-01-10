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
import xtdb.api.query.Query;

import java.io.IOException;
import java.util.List;

public class UnifyDeserializer extends StdDeserializer<Query.Unify> {

    public UnifyDeserializer() {
        super(Query.Unify.class);
    }

    @Override
    public Query.Unify deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        if (!node.isObject() || !node.has("unify") || !node.get("unify").isArray()) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-unify"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        return Query.unify(mapper.treeToValue(node.get("unify"), mapper.getTypeFactory().constructCollectionType(List.class, Query.UnifyClause.class)));
    }
}
