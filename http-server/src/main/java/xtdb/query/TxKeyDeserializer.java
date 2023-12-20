package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.api.TransactionKey;

import java.io.IOException;
import java.time.Instant;

public class TxKeyDeserializer extends StdDeserializer<TransactionKey> {

    public TxKeyDeserializer() {
        super(TransactionKey.class);
    }

    public TransactionKey deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.isObject() || !node.has("tx_id") || !node.has("system_time")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-tx-key"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        var instant = codec.treeToValue(node.get("system_time"), Object.class);
        if (instant instanceof Instant) {
            return new TransactionKey(node.get("tx_id").asLong(), (Instant) instant);
        }
        throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-tx-key"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
    }
}