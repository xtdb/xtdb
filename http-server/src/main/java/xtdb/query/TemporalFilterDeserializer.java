package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.List;

public class TemporalFilterDeserializer extends StdDeserializer<TemporalFilter> {

    static final String ALL_TIME_STR = "all_time";

    public TemporalFilterDeserializer() {
        super(TemporalFilter.class);
    }

    @Override
    public TemporalFilter deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        if (node.isTextual()){
            if (!ALL_TIME_STR.equals(node.asText())) {
                throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-temporal-filter"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            return TemporalFilter.ALL_TIME;
        }
        // TODO check parsed Expr for correct type (Date, Instant, ZonedDateTime)
        if (node.isObject()) {
            if (node.has("at")){
                return TemporalFilter.at(mapper.treeToValue(node.get("at"), Expr.class));
            }
            if (node.has("from")){
                return TemporalFilter.from(mapper.treeToValue(node.get("from"), Expr.class));
            }
            if (node.has("to")){
                return TemporalFilter.to(mapper.treeToValue(node.get("to"), Expr.class));
            }
            if (node.has("in")) {
                var inNode = node.get("in");
                if (!inNode.isArray() || inNode.size() != 2) {
                    throw new IllegalArgumentException("In TemporalFilter expects array of 2 timestamps", PersistentHashMap.create(Keyword.intern("json"), node.get("in").toPrettyString()), null);
                }
                List<Expr> exprs = mapper.treeToValue(inNode, mapper.getTypeFactory().constructCollectionType(List.class, Expr.class));
                return TemporalFilter.in(exprs.get(0), exprs.get(1));
            }
        }
        throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-temporal-filter"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
    }
}