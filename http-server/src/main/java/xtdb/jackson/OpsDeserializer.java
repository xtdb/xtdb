package xtdb.jackson;


import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.*;

import java.io.IOException;

public class OpsDeserializer extends StdDeserializer<Ops>  {

    public OpsDeserializer() {
        super(Ops.class);
    }

    @Override
    public Ops deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        ObjectNode node = codec.readTree(p);

        if (node.has("put")) {
            return codec.treeToValue(node, Put.class);
        } else if (node.has("delete")) {
            return codec.treeToValue(node, Delete.class);
        } else if (node.has("erase")) {
            return codec.treeToValue(node, Erase.class);
        } else if (node.has("call")) {
            return codec.treeToValue(node, Call.class);
        } else {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-tx-op"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}