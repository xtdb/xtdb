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
import xtdb.api.TransactionKey;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryMapDeserializer extends StdDeserializer<QueryMap> {

    public static Keyword snakeCase = Keyword.intern("snake_case");

    public QueryMapDeserializer() {
        super(QueryMap.class);
    }

    public QueryMap deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isObject()) {
                Query query;
                if (node.has("query")) {
                    query = mapper.treeToValue(node.get("query"), Query.class);
                } else {
                    throw IllegalArgumentException.create(Keyword.intern("xtql", "missing-query"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
                }

                // TODO this only works in connection with keyword deserialization
                Map<Keyword, Object> args =  null;
                if (node.has("args")) {
                    args = (Map<Keyword, Object>) mapper.treeToValue(node.get("args"), Object.class);
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

                Keyword keyFn = snakeCase;
                if (node.has("key_fn")) {
                    keyFn = (Keyword) mapper.treeToValue(node.get("key_fn"), Object.class);;
                }

                return new QueryMap(query, args, basis, afterTx, txTimeout, defaultTz, explain, keyFn);
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