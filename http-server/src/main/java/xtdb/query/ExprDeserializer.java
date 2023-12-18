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
import java.util.*;
import java.util.function.BiFunction;

public class ExprDeserializer extends StdDeserializer<Expr> {

    static String xtSetType = "xt:set";

    public ExprDeserializer() {
        super(Expr.class);
    }

    private Expr deserializeSubquery(ObjectMapper mapper, JsonNode node, BiFunction<Query,List<ArgSpec>,Expr> createSubquery) throws IllegalArgumentException, JsonProcessingException{
        if (node instanceof ObjectNode && node.has("query") && node.has("bind")) {
            return createSubquery.apply(mapper.treeToValue(node.get("query"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("bind"), ArgSpec::of));
        } else {
            throw new IllegalArgumentException("Subquery expects object node with 'q' and 'bind' keys.", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
    }

    @Override
    public Expr deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isNull()) {
                return Expr.NULL;
            }
            if (node.isTextual()) {
                Expr.val(node.asText());
            }
            if (node.isBoolean()) {
                return node.asBoolean() ? Expr.TRUE : Expr.FALSE;
            }
            if (node.isInt()) {
                return Expr.val((long) node.asInt());
            }
            if (node.isLong() || node.isInt()) {
                return Expr.val(node.asLong());
            }
            if (node.isDouble()) {
                return Expr.val(node.asDouble());
            }
            if (node.isArray()) {
                return Expr.list(mapper.treeToValue(node, typeFactory.constructCollectionType(List.class, Expr.class)));
            }

            if (node instanceof ObjectNode) {
                if (node.has("xt:exists")) {
                    return deserializeSubquery(mapper, node.get("xt:exists"), Expr::exists);
                }
                if (node.has("xt:q")) {
                    return deserializeSubquery(mapper, node.get("xt:q"), Expr::q);
                }
                if (node.has("xt:pull")) {
                    return deserializeSubquery(mapper, node.get("xt:pull"), Expr::pull);
                }
                if (node.has("xt:pull_many")) {
                    return deserializeSubquery(mapper, node.get("xt:pull_many"), Expr::pullMany);
                }
                if (node.has("xt:call")) {
                    JsonNode callNode = node.get("xt:call");

                    if (callNode instanceof ObjectNode && callNode.has("f")) {
                        List<Expr> args = new ArrayList<>();
                        for (JsonNode argSpecNode : (ArrayNode) callNode.get("args")) {
                            args.add(mapper.treeToValue(argSpecNode, Expr.class));
                        }
                        return Expr.call(callNode.get("f").asText(), args);
                    } else {
                        throw new IllegalArgumentException("Call expects object node with 'f' key and optional 'args' key.", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
                    }
                }
                if (node.has("xt:lvar")) {
                    return Expr.lVar(node.get("xt:lvar").asText());
                }
                if (node.has("xt:param")) {
                    return Expr.param(node.get("xt:param").asText());
                }

                // JSON LD
                if (node.has("@type")) {
                    // composite types need to be treated specially
                    if (xtSetType.equals(node.get("@type").asText())) {
                        return Expr.set(new HashSet<Expr>(mapper.treeToValue(node.get("@value"), typeFactory.constructCollectionType(List.class, Expr.class))));
                    } else {
                        return Expr.val(mapper.treeToValue(node, Object.class));
                    }
                }
                // normal maps need to be treated specially
                Map<String, Expr> mapExprs = new HashMap<>();
                Iterator<Map.Entry<String, JsonNode>> itr = node.fields();
                while (itr.hasNext()) {
                    Map.Entry<String, JsonNode> entry = itr.next();
                    mapExprs.put(entry.getKey(), mapper.treeToValue(entry.getValue(), Expr.class));
                }
                return Expr.map(mapExprs);
            }
            return Expr.val(mapper.treeToValue(node, Object.class));

        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-expr"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}