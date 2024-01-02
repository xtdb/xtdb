package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.Call;

import java.io.IOException;
import java.util.List;

public class CallDeserializer extends StdDeserializer<Call> {

    public CallDeserializer() {
        super(Call.class);
    }

    @Override
    public Call deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec =  p.getCodec();
        BaseJsonNode node = codec.readTree(p);

        if (!node.isObject() || !node.get("args").isArray()){
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-call"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        return new Call(codec.treeToValue(node.get("call"), Object.class), codec.treeToValue(node.get("args"), List.class));
    }
}
