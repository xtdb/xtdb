package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Binding;
import xtdb.api.query.Expr;
import xtdb.api.query.Query;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;

public class ExprDeserializer extends StdDeserializer<Expr> {

    static String xtSetType = "xt:set";

    public ExprDeserializer() {
        super(Expr.class);
    }

    private Expr deserializeSubquery(ObjectMapper mapper, JsonNode node, BiFunction<Query, List<Binding>, Expr> createSubquery) throws IllegalArgumentException, JsonProcessingException {
        if (node.isObject() && node.has("query") && node.has("bind")) {
            return createSubquery.apply(mapper.treeToValue(node.get("query"), Query.class), SpecListDeserializer.nodeToSpecs(mapper, node.get("bind"), Binding::new));
        } else {
            throw IllegalArgumentException.createNoKey("Subquery expects object node with 'q' and 'bind' keys.", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expr deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        if (node.isNull()) {
            return Expr.NULL;
        }
        if (node.isTextual()) {
            return Expr.val(node.asText());
        }
        if (node.isBoolean()) {
            return node.asBoolean() ? Expr.TRUE : Expr.FALSE;
        }
        if (node.isLong() || node.isInt()) {
            return Expr.val(node.asLong());
        }
        if (node.isDouble()) {
            return Expr.val(node.asDouble());
        }
        if (node.isArray()) {
            return Expr.list((List<? extends Expr>) mapper.treeToValue(node, typeFactory.constructCollectionType(List.class, Expr.class)));
        }

        if (node.isObject()) {
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

                if (callNode.isObject() && callNode.has("f")) {
                    List<Expr> args = new ArrayList<>();
                    if (callNode.has("args")) {
                        if (!callNode.get("args").isArray()) {
                            throw IllegalArgumentException.createNoKey("Call args need to be a list.", PersistentHashMap.create(Keyword.intern("json"), callNode.toPrettyString()), null);
                        }
                        for (JsonNode argSpecNode : callNode.get("args")) {
                            args.add(mapper.treeToValue(argSpecNode, Expr.class));
                        }
                    }
                    return Expr.call(callNode.get("f").asText(), args);
                } else {
                    throw IllegalArgumentException.createNoKey("Call expects object node with 'f' key and optional 'args' key.", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
                }
            }
            if (node.has("xt:lvar")) {
                var lVarNode = node.get("xt:lvar");
                if (!lVarNode.isTextual()) {
                    throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-lvar"), PersistentHashMap.create(Keyword.intern("json"), lVarNode.toPrettyString()));
                }
                return Expr.lVar(lVarNode.asText());
            }
            if (node.has("xt:param")) {
                var paramNode = node.get("xt:param");
                if (!paramNode.isTextual()) {
                    throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-param"), PersistentHashMap.create(Keyword.intern("json"), paramNode.toPrettyString()));
                }
                return Expr.param(paramNode.asText());
            }

            // JSON LD
            if (node.has("@type") && node.has("@value")) {
                var typeNode = node.get("@type");
                if (!typeNode.isTextual()) {
                    throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-json-ld"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
                }
                // composite types need to be treated specially
                if (xtSetType.equals(typeNode.asText())) {
                    return Expr.set((List<? extends Expr>) mapper.treeToValue(node.get("@value"), typeFactory.constructCollectionType(List.class, Expr.class)));
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
    }

    @Override
    public Expr getNullValue(DeserializationContext ctxt) {
        return Expr.NULL;
    }
}
