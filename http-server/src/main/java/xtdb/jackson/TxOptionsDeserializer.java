package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.TxOptions;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;

public class TxOptionsDeserializer extends StdDeserializer<TxOptions> {

    public TxOptionsDeserializer() {
        super(TxOptions.class);
    }

    @Override
    public TxOptions deserialize(com.fasterxml.jackson.core.JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = jp.getCodec();

        ObjectNode node = codec.readTree(jp);

        if (!node.isObject()){
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx-options"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        Instant systemTime = null;
        ZoneId defaultTz = null;
        if (node.has("system_time")) {
            var candidateSystemTime = codec.readValue(node.get("system_time").traverse(codec), Object.class);
            if (!(candidateSystemTime instanceof Instant)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx-options"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            systemTime = (Instant) candidateSystemTime;
        }
        if (node.has("default_tz")) {
            var candidateZoneId = codec.readValue(node.get("default_tz").traverse(codec), Object.class);
            if (!(candidateZoneId instanceof ZoneId)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx-options"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            defaultTz = (ZoneId) candidateZoneId;
        }
        return new TxOptions(systemTime, defaultTz);
    }
}