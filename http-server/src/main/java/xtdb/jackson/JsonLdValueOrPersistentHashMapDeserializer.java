package xtdb.jackson;

import clojure.lang.ITransientMap;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class JsonLdValueOrPersistentHashMapDeserializer extends StdDeserializer<Object> implements ContextualDeserializer {

    private final Map<String, Class<?>> typeMapping;
    private KeyDeserializer _keyDeserializer;
    private JsonDeserializer<?> _valueDeserializer;

    public JsonLdValueOrPersistentHashMapDeserializer(Map<String, Class<?>> typeMapping) {
        super(Map.class);
        this.typeMapping = typeMapping;
    }

    public JsonLdValueOrPersistentHashMapDeserializer(KeyDeserializer keyDeser, JsonDeserializer<?> valueDeser, Map<String, Class<?>> typeMapping) {
        this(typeMapping);
        _keyDeserializer = keyDeser;
        _valueDeserializer = valueDeser;
    }

    protected JsonLdValueOrPersistentHashMapDeserializer withResolved(KeyDeserializer keyDeser, JsonDeserializer<?> valueDeser) {
        return this._keyDeserializer == keyDeser && this._valueDeserializer == valueDeser ? this : new JsonLdValueOrPersistentHashMapDeserializer(keyDeser, valueDeser, this.typeMapping);
    }

    @Override
    public JsonDeserializer<Object> createContextual(DeserializationContext ctxt, BeanProperty beanProperty) throws JsonMappingException {
        JavaType object = ctxt.constructType(Object.class);
        KeyDeserializer keyDeser = ctxt.findKeyDeserializer(object, null);
        JsonDeserializer<Object> valueDeser = ctxt.findNonContextualValueDeserializer(object);
        return this.withResolved(keyDeser, valueDeser);
    }

    @Override
    public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        ObjectCodec codec = jp.getCodec();
        ObjectNode node = codec.readTree(jp);

        if (node.has("@type")) {
            Class<?> decodeType = typeMapping.get(node.get("@type").asText());
            if (decodeType != null) {
                return codec.readValue(node.get("@value").traverse(codec), decodeType);
            }
        }

        ITransientMap t = PersistentHashMap.EMPTY.asTransient();
        Iterator<Map.Entry<String,JsonNode>> entries = node.fields();
        while (entries.hasNext()) {
            var entry = entries.next();
            var key = _keyDeserializer.deserializeKey(entry.getKey() , ctxt);
            var value = codec.readValue(entry.getValue().traverse(codec), Object.class);
            t = t.assoc(key, value);
        }

        return t.persistent();
    }
}
