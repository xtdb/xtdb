package xtdb.jackson;

import clojure.lang.IFn;
import clojure.lang.ITransientMap;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JsonLdValueOrPersistentHashMapDeserializer extends StdDeserializer<Object> implements ContextualDeserializer {
    private final Map<String, IFn> decoders;
    private KeyDeserializer _keyDeserializer;
    private JsonDeserializer<?> _valueDeserializer;

    public JsonLdValueOrPersistentHashMapDeserializer(Map<String, IFn> decoders) {
        super(Map.class);
        this.decoders = decoders;
    }

    public JsonLdValueOrPersistentHashMapDeserializer(KeyDeserializer keyDeser, JsonDeserializer<?> valueDeser, Map<String, IFn> decoders) {
        this(decoders);
        _keyDeserializer = keyDeser;
        _valueDeserializer = valueDeser;
    }

    protected JsonLdValueOrPersistentHashMapDeserializer withResolved(KeyDeserializer keyDeser, JsonDeserializer<?> valueDeser) {
        return this._keyDeserializer == keyDeser && this._valueDeserializer == valueDeser ? this : new JsonLdValueOrPersistentHashMapDeserializer(keyDeser, valueDeser, this.decoders);
    }

    @Override
    public JsonDeserializer<Object> createContextual(DeserializationContext ctxt, BeanProperty beanProperty) throws JsonMappingException {
        JavaType object = ctxt.constructType(Object.class);
        KeyDeserializer keyDeser = ctxt.findKeyDeserializer(object, null);
        JsonDeserializer<Object> valueDeser = ctxt.findNonContextualValueDeserializer(object);
        return this.withResolved(keyDeser, valueDeser);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec mapper = jp.getCodec();
        ObjectNode node = mapper.readTree(jp);

        if (node.has("@type")) {
            IFn decode = this.decoders.get(node.get("@type").asText());
            if (decode != null) {
                return decode.invoke(mapper.readValue(node.get("@value").traverse(mapper), Object.class));
            }
        }

        ITransientMap t = PersistentHashMap.EMPTY.asTransient();
        Iterator<Map.Entry<String,JsonNode>> entries = node.fields();
        while (entries.hasNext()) {
            var entry = entries.next();
            var key = _keyDeserializer.deserializeKey(entry.getKey() , ctxt);
            //var value = _valueDeserializer.deserialize(entry.getValue().traverse(), ctxt);
            var value = mapper.readValue(entry.getValue().traverse(mapper), Object.class);
            t = t.assoc(key, value);
        }

        return t.persistent();
    }
}
