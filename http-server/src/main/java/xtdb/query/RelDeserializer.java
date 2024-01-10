package xtdb.query;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.api.query.Binding;
import xtdb.api.query.Expr;
import xtdb.api.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RelDeserializer extends StdDeserializer<Query.Relation> {

    public RelDeserializer() {
        super(Query.Relation.class);
    }

    private List<Map<String, Expr>> deserializeDocuments (ObjectCodec codec, ArrayNode node) throws JsonProcessingException {
        List<Map<String, Expr>> res = new ArrayList<>();
        for (JsonNode documentNode: node) {
            Map<String, Expr> document = new HashMap <>();
            var itr = documentNode.fields();
            while(itr.hasNext()) {
                var entry = itr.next();
                document.put(entry.getKey(), codec.treeToValue(entry.getValue(), Expr.class));
            }
            res.add(document);
        }
        return res;
    }
    private List<Binding> deserializeBind(ObjectMapper mapper, ArrayNode node) throws Exception {
        return SpecListDeserializer.<Binding>nodeToSpecs(mapper, node, Binding::new);
    }
    public Query.Relation deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        ObjectNode node = codec.readTree(p);

        var relNode = node.get("rel");
        Query.Relation rel;
        if (relNode.isArray()) {
            rel = Query.relation(deserializeDocuments(codec, (ArrayNode) relNode), SpecListDeserializer.<Binding>nodeToSpecs(codec, node.get("bind"), Binding::new));
        } else {
            rel = Query.relation((Expr.Param) codec.treeToValue(relNode, Expr.class), SpecListDeserializer.<Binding>nodeToSpecs(codec, node.get("bind"), Binding::new));
        }
        return rel;
    }
}
