package xtdb.jackson;

import clojure.lang.IExceptionInfo;
import clojure.lang.Keyword;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import xtdb.IllegalArgumentException;
import xtdb.RuntimeException;

import java.io.IOException;
import java.time.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JsonLdModule extends SimpleModule {

    private interface Encoder<T> {
        Encoder<Object> DEFAULT_ENCODER = (val, gen, sp) -> gen.writeString(val.toString());

        void encode(T value, JsonGenerator gen, SerializerProvider serializers) throws IOException;
    }

    private static class JsonLdSerializer<T> extends JsonSerializer<T> {

        private final String tag;
        private final Encoder<? super T> encoder;

        public JsonLdSerializer(String tag, Encoder<? super T> encoder) {
            this.tag = tag;
            this.encoder = encoder;
        }

        @Override
        public void serialize(T value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();
            gen.writeStringField("@type", tag);
            gen.writeFieldName("@value");
            encoder.encode(value, gen, serializers);
            gen.writeEndObject();
        }
    }

    private final Map<String, Class<?>> typeMapping = new HashMap<>();

    private <T> void addType(Class<T> type, String tag, Encoder<? super T> encoder) {
        addSerializer(type, new JsonLdSerializer<>(tag, encoder));
        typeMapping.put(tag, type);
    }

    private <T> void addType(Class<T> type, String tag) {
        addType(type, tag, Encoder.DEFAULT_ENCODER);
    }

    @SuppressWarnings("unchecked")
    private static <T> JsonDeserializer<T> mapDeserializer(Map<String, Class<?>> typeMapping) {
        // HACK. this casting magic allows us to submit a `JsonDeserializer<Object>` to deserialize `Maps`.
        return (JsonDeserializer<T>) new JsonLdValueOrPersistentHashMapDeserializer(typeMapping);
    }

    public JsonLdModule() {
        super("JSON-LD");
        addType(Keyword.class, "xt:keyword", (val, gen, sp) -> gen.writeString(val.sym.toString()));
        addDeserializer(Keyword.class, new JsonDeserializer<>() {
            @Override
            public Keyword deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                return Keyword.intern(p.readValueAs(String.class));
            }
        });

        addType(Set.class, "xt:set", ((value, gen, serializers) -> {
            gen.writeStartArray();
            for (Object o : value) {
                gen.writeObject(o);
            }
            gen.writeEndArray();
        }));

        addType(Date.class, "xt:instant", (val, gen, sp) -> Encoder.DEFAULT_ENCODER.encode(val.toInstant(), gen, sp));
        addType(Instant.class, "xt:instant");
        addType(LocalDate.class, "xt:date");
        addType(LocalDateTime.class, "xt:timestamp");
        addType(ZonedDateTime.class, "xt:timestamptz");
        addType(Duration.class, "xt:duration");
        addType(ZoneId.class, "xt:timezone");

        addType(Throwable.class, "xt:error", (v, gen, sp) -> {
            gen.writeStartObject();
            gen.writeStringField("xtdb.error/message", v.getMessage());
            gen.writeStringField("xtdb.error/class", v.getClass().getName());
            if (v instanceof IExceptionInfo ei) {
                gen.writeFieldName("xtdb.error/data");
                gen.writeObject(ei.getData());
            }
            if (v instanceof IllegalArgumentException iae) {
                var k = iae.getKey();
                if (k != null) {
                    gen.writeFieldName("xtdb.error/error-key");
                    gen.writeObject(k.sym.toString());
                }
            }
            if (v instanceof RuntimeException runEx) {
                var k = runEx.getKey();
                if (k != null) {
                    gen.writeFieldName("xtdb.error/error-key");
                    gen.writeObject(k.sym.toString());
                }
            }
            gen.writeEndObject();
        });

        addDeserializer(Throwable.class, new ThrowableDeserializer());

        addDeserializer(Map.class, mapDeserializer(typeMapping));
    }

}
