package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;
import xtdb.api.TransactionKey;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;

import static xtdb.query.QueryOpts.queryOpts;

public class QueryOptsDeserializer extends StdDeserializer<QueryOpts> {

    public QueryOptsDeserializer() {
        super(QueryOpts.class);
    }

    public QueryOpts deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        if (!node.isObject()) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-map"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        var builder = queryOpts();

        if (node.has("args")) {
            builder.args(mapper.treeToValue(node.get("args"), typeFactory.constructMapType(Map.class, String.class, Object.class)));
        }

        if (node.has("basis")) {
            builder.basis(mapper.treeToValue(node.get("basis"), Basis.class));
        }

        if (node.has("after_tx")) {
            builder.afterTx(mapper.treeToValue(node.get("after_tx"), TransactionKey.class));
        }

        if (node.has("tx_timeout")) {
            builder.txTimeout((Duration) mapper.treeToValue(node.get("tx_timeout"), Object.class));
        }

        if (node.has("default_tz")) {
            builder.defaultTz((ZoneId) mapper.treeToValue(node.get("default_tz"), Object.class));
        }

        if (node.has("explain")) {
            builder.explain(node.get("explain").asBoolean());
        }

        if (node.has("key_fn")) {
            builder.keyFn(node.get("key_fn").asText());
        }

        return builder.build();
    }
}
