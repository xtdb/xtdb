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
import java.util.List;

public class ExprDeserializer extends StdDeserializer<Expr> {

    public ExprDeserializer() {
        super(Expr.class);
    }

    private boolean supportedObjectDeserialization(ObjectNode node) {
        return node.has("exists") || node.has("q") || node.has("call");
    }

   private List<ArgSpec> deserializeBind(ObjectMapper mapper, ArrayNode node) throws JsonProcessingException {
        List<ArgSpec> res = new ArrayList<>();
        for (JsonNode argSpecNode : node) {
            res.add(mapper.treeToValue(argSpecNode, ArgSpec.class));
        }
        return res;
   }

    @Override
    public Expr deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        //TODO this currently does not deal with String and Map literals as we still need to decide on syntax here
        //TODO pull, pull-many, param
        try {
            if (node.isTextual()) {
                return Expr.lVar(node.asText());
            }
            if (node.isBoolean()) {
                return node.asBoolean() ? Expr.TRUE : Expr.FALSE;
            }
            if (node.isLong()) {
                return Expr.val(node.asLong());
            }
            if (node.isDouble()) {
                return Expr.val(node.asDouble());
            }
            if (node.isObject() && supportedObjectDeserialization((ObjectNode) node)){
                ObjectNode objectNode = (ObjectNode) node;
                if (node.has("exists")) {
                    return Expr.exists(mapper.treeToValue(objectNode.get("exists"), Query.class), deserializeBind(mapper, (ArrayNode) objectNode.get("bind")));
                }
                if (node.has("q")) {
                    return Expr.q(mapper.treeToValue(objectNode.get("q"), Query.class), deserializeBind(mapper, (ArrayNode) objectNode.get("bind")));
                }
                if (node.has("call")) {
                    List<Expr> args = new ArrayList<>();
                    for (JsonNode argSpecNode : (ArrayNode) node.get("args")) {
                        args.add(mapper.treeToValue(argSpecNode, Expr.class));
                    }
                    return Expr.call(node.get("call").asText(), args);
                }
            }
            return Expr.val(mapper.treeToValue(node, Object.class));
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-expr"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}
