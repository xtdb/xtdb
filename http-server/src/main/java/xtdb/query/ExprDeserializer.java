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

public class ExprDeserializer extends StdDeserializer<Expr> {

    static String xtSetType = "xt:set";

    public ExprDeserializer() {
        super(Expr.class);
    }

    @Override
    public Expr deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        //TODO this currently does not deal with String literals, as we still need to decide on syntax here
        try {
            if (node.isNull()) {
                return Expr.NULL;
            }
            if (node.isTextual()) {
                var symbolOrParam = node.asText();
                if (symbolOrParam.startsWith("$")) {
                    return Expr.param(symbolOrParam);
                }
                return Expr.lVar(symbolOrParam);
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

            // TODO some way to differentiate special syntax (exits, pull, call...) from literal maps
            if (node instanceof ObjectNode) {
                if (node.has("xt:exists")) {
                    return Expr.exists(mapper.treeToValue(node.get("xt:exists"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("bind"), ArgSpec::of));
                }
                if (node.has("xt:q")) {
                    return Expr.q(mapper.treeToValue(node.get("xt:q"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("bind"), ArgSpec::of));
                }
                if (node.has("xt:pull")) {
                    return Expr.pull(mapper.treeToValue(node.get("xt:pull"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("bind"), ArgSpec::of));
                }
                if (node.has("xt:pull_many")) {
                    return Expr.pullMany(mapper.treeToValue(node.get("xt:pull_many"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("bind"), ArgSpec::of));
                }
                if (node.has("xt:call")) {
                    List<Expr> args = new ArrayList<>();
                    for (JsonNode argSpecNode : (ArrayNode) node.get("args")) {
                        args.add(mapper.treeToValue(argSpecNode, Expr.class));
                    }
                    return Expr.call(node.get("xt:call").asText(), args);
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
