package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SpecListDeserializer {

    public static <T> List<T> nodeToSpecs(ObjectMapper mapper, JsonNode node, BiFunction<String, Expr, T> ofFn) throws JsonProcessingException, IllegalArgumentException {
        List <T> specs = new ArrayList<>();

        if (node.isArray()) {
            for (JsonNode itemNode : node) {
                if (itemNode.isTextual()) {
                    String var = itemNode.asText();
                    Expr expr = Expr.lVar(var);
                    specs.add(ofFn.apply(var, expr));
                } else if (itemNode.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> itr = itemNode.fields();
                    while (itr.hasNext()) {
                        Map.Entry<String, JsonNode> entry = itr.next();
                        specs.add(ofFn.apply(entry.getKey(), mapper.treeToValue(entry.getValue(), Expr.class)));
                    }
                } else {
                    throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-spec"), PersistentHashMap.create(Keyword.intern("json"), itemNode.toPrettyString()));
                }
            }
        } else {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        return specs;
    }
}