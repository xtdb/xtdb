package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.tx.Ops;
import xtdb.tx.Tx;
import xtdb.tx.TxOptions;

import java.io.IOException;
import java.util.List;

public class TxDeserializer extends StdDeserializer<Tx>  {

    public TxDeserializer() {
        super(Tx.class);
    }

    @Override
    public Tx deserialize(com.fasterxml.jackson.core.JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();
        JsonNode node = mapper.readTree(jp);

        if (!node.isObject()) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        if (!node.has("tx_ops")) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "missing-tx-ops"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        if (!node.get("tx_ops").isArray()) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx-ops"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        List<Ops> ops = mapper.treeToValue(node.get("tx_ops"), mapper.getTypeFactory().constructCollectionType(List.class, Ops.class));
        TxOptions txOptions = null;
        if (node.has("tx_options")) {
            txOptions = mapper.treeToValue(node.get("tx_options"), TxOptions.class);
        }

        return new Tx(ops, txOptions);
    }
}