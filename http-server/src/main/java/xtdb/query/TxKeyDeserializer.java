package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.time.Instant;

public class TxKeyDeserializer extends StdDeserializer<TransactionKey> {

    public TxKeyDeserializer() {
        super(TransactionKey.class);
    }

    public TransactionKey deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isObject()) {
                return new TransactionKey(node.get("tx_id").asLong(), (Instant) mapper.treeToValue(node.get("system_time"), Object.class));
            } else {
                throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-tx-key"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-tx-key"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}