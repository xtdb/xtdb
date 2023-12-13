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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RelDeserializer extends StdDeserializer<Query.Relation> {

    public RelDeserializer() {
        super(Query.Relation.class);
    }

    private List<Map<String, Expr>> deserializeDocuments (ObjectMapper mapper, ArrayNode node) throws JsonProcessingException {
        List<Map<String, Expr>> res = new ArrayList<>();
        for (JsonNode documentNode: node) {
            Map<String, Expr> document = new HashMap <>();
            var itr = documentNode.fields();
            while(itr.hasNext()) {
                var entry = itr.next();
                document.put(entry.getKey(), mapper.treeToValue(entry.getValue(), Expr.class));
            }
            res.add(document);
        }
        return res;
    }
    private List<OutSpec> deserializeBind(ObjectMapper mapper, ArrayNode node) throws Exception {
        return SpecListDeserializer.nodeToOutSpecs(mapper, node);
    }
    public Query.Relation deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        try {
            var relNode = node.get("rel");
            Query.Relation rel;
            if (relNode.isArray()) {
                rel = Query.relation(deserializeDocuments(mapper, (ArrayNode) node.get("rel")), deserializeBind(mapper, (ArrayNode) node.get("bind")));
            } else {
                rel = Query.relation((Expr.Param) mapper.treeToValue(node.get("rel"), Expr.class), deserializeBind(mapper, (ArrayNode) node.get("bind")));
            }
            return rel;
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-rel"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}