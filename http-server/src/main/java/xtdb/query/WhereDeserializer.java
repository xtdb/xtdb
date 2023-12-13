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

public class WhereDeserializer extends StdDeserializer<Query.Where> {

    public WhereDeserializer() {
        super(Query.Where.class);
    }

    @Override
    public Query.Where deserialize(JsonParser p, DeserializationContext ctxt) 
            throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            JsonNode where = node.get("where");

            if (where.isArray()) {
                List<Expr> predicates = new ArrayList<>();

                for (JsonNode predicateNode : where) {
                    Expr predicate = mapper.treeToValue(predicateNode, Expr.class);
                    predicates.add(predicate);
                }

                return Query.where(predicates);
            } else {
                throw new Exception("Where should be a list of expressions");
            }
            
        } catch (Exception e)  {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-where"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
