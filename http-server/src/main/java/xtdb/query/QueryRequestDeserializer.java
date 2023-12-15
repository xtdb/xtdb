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

public class QueryRequestDeserializer extends StdDeserializer<QueryRequest> {

    public QueryRequestDeserializer() {
        super(QueryRequest.class);
    }

    public QueryRequest deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isObject()) {
                Query query;
                if (node.has("query")) {
                    query = mapper.treeToValue(node.get("query"), Query.class);
                } else {
                    throw IllegalArgumentException.create(Keyword.intern("xtql", "missing-query"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
                }

                Map<String, Object> args =  null;
                if (node.has("args")) {
                    args = mapper.treeToValue(node.get("args"), typeFactory.constructMapType(Map.class, String.class, Object.class));
                }

                Basis basis = null;
                if (node.has("basis")) {
                    basis = mapper.treeToValue(node.get("basis"), Basis.class);
                }

                TransactionKey afterTx = null;
                if (node.has("after_tx")) {
                    afterTx = mapper.treeToValue(node.get("after_tx"), TransactionKey.class);
                }

                Duration txTimeout = null;
                if (node.has("tx_timeout")) {
                    txTimeout = (Duration) mapper.treeToValue(node.get("tx_timeout"), Object.class);
                }

                ZoneId defaultTz = null;
                if (node.has("default_tz")) {
                    defaultTz = (ZoneId) mapper.treeToValue(node.get("default_tz"), Object.class);
                }

                Boolean explain = false;
                if (node.has("explain")) {
                    explain = node.get("explain").asBoolean();
                }

                String keyFn = "snake_case";
                if (node.has("key_fn")) {
                    keyFn = node.get("key_fn").asText();
                }

                return new QueryRequest(query, args, basis, afterTx, txTimeout, defaultTz, explain, keyFn);
            } else  {
                throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-map"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException e) {
          throw e;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-map"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}