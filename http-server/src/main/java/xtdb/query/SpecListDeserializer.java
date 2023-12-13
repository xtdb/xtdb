package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SpecListDeserializer {
    final static class AttrExpr {
        public final String attr; 
        public final Expr expr; 

        private AttrExpr(String attr, Expr expr) { 
            this.attr = attr; 
            this.expr = expr; 
        }
    }

    private static List<AttrExpr> processItemNode(ObjectMapper mapper, JsonNode itemNode) throws Exception {
        List<AttrExpr> specs = new ArrayList<>();

        if (itemNode.isTextual()) {
            String var = itemNode.asText();
            Expr expr = Expr.lVar(var);
            AttrExpr attrExpr = new AttrExpr(var, expr); 
            specs.add(attrExpr);
        } else if (itemNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) itemNode;
            Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();
            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();
                AttrExpr attrExpr = new AttrExpr(entry.getKey(), mapper.treeToValue(entry.getValue(), Expr.class));
                specs.add(attrExpr);
            }
        } else {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-spec"), PersistentHashMap.create(Keyword.intern("json"), itemNode.toPrettyString()));
        }

        return specs;
    }

    public static List<AttrExpr> nodeToAttrExprs(ObjectMapper mapper, JsonNode node) throws Exception {
        List <AttrExpr> specs = new ArrayList<>();

        if (node.isArray()) {
            for (JsonNode itemNode : node) {
                specs.addAll(processItemNode(mapper, itemNode));
            }
        } else {
            specs.addAll(processItemNode(mapper, node));
        }

        return specs;

    }

    public static List<ArgSpec> nodeToArgSpecs(ObjectMapper mapper, JsonNode node) throws Exception { 
        List<AttrExpr> attrExprs = nodeToAttrExprs(mapper, node);
        List<ArgSpec> argSpecs = new ArrayList<>();

        for (AttrExpr attrExpr : attrExprs) {
            argSpecs.add(ArgSpec.of(attrExpr.attr, attrExpr.expr));
        }

        return argSpecs;
    }

    public static List<OutSpec> nodeToOutSpecs(ObjectMapper mapper, JsonNode node) throws Exception { 
        List<AttrExpr> attrExprs = nodeToAttrExprs(mapper, node);
        List<OutSpec> outSpecs = new ArrayList<>();

        for (AttrExpr attrExpr : attrExprs) {
            outSpecs.add(OutSpec.of(attrExpr.attr, attrExpr.expr));
        }

        return outSpecs;
    }

    public static List<VarSpec> nodeToVarSpecs(ObjectMapper mapper, JsonNode node) throws Exception { 
        List<AttrExpr> attrExprs = nodeToAttrExprs(mapper, node);
        List<VarSpec> varSpecs = new ArrayList<>();

        for (AttrExpr attrExpr : attrExprs) {
            varSpecs.add(VarSpec.of(attrExpr.attr, attrExpr.expr));
        }

        return varSpecs;
    }

    public static List<ColSpec> nodeToColSpecs(ObjectMapper mapper, JsonNode node) throws Exception { 
        List<AttrExpr> attrExprs = nodeToAttrExprs(mapper, node);
        List<ColSpec> colSpecs = new ArrayList<>();

        for (AttrExpr attrExpr : attrExprs) {
            colSpecs.add(ColSpec.of(attrExpr.attr, attrExpr.expr));
        }

        return colSpecs;
    }
}
