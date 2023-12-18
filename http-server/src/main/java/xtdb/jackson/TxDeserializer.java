package xtdb.jackson;

import clojure.lang.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

        ObjectNode node = mapper.readTree(jp);
        List<Ops> ops = null;
        TxOptions txOptions = null;

        try {
            if (node.has("tx_ops")) {
                ops = mapper.treeToValue(node.get("tx_ops"), mapper.getTypeFactory().constructCollectionType(List.class, Ops.class));
            } else {
                throw IllegalArgumentException.create(Keyword.intern("tx", "missing-tx-ops"), PersistentHashMap.EMPTY);
            }
            if (node.has("tx_options")) {
                txOptions = mapper.treeToValue(node.get("tx_options"), TxOptions.class);
            }
        } catch (IllegalArgumentException i) {
           throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        return new Tx(ops, txOptions);
    }
}