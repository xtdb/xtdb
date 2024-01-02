package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.Erase;

import java.io.IOException;

public class EraseDeserializer extends StdDeserializer<Erase> {

    public EraseDeserializer() {
        super(Erase.class);
    }

    @Override
    public Erase deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        BaseJsonNode node = codec.readTree(p);

        try {
            ObjectNode objectNode = (ObjectNode) node;
            return new Erase(Keyword.intern(objectNode.get("erase").asText()), codec.treeToValue(objectNode.get("id"), Object.class));
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-erase"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }  
    }
}
