package crux.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings("unused") // entry points
public final class ModuleConfiguration {
    private static final String MODULE = "crux/module";
    static final ModuleConfiguration EMPTY = new ModuleConfiguration(new HashMap<>());

    private final Map<String, Object> opts;

    public static ModuleConfiguration buildModule(Consumer<Builder> f) {
        Builder builder = new Builder();
        f.accept(builder);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private ModuleConfiguration(Map<String, Object> opts) {
        this.opts = opts;
    }

    public final static class Builder {
        private final Map<String, Object> opts = new HashMap<>();

        public Builder set(String key, Object value) {
            opts.put(key, value);
            return this;
        }

        public Builder module(String module) {
            return set(MODULE, module);
        }

        public Builder set(Map<String, Object> options) {
            opts.putAll(options);
            return this;
        }

        public Builder with(String module) {
            return set(module, ModuleConfiguration.EMPTY);
        }

        public Builder with(String module, ModuleConfiguration configuration) {
            return set(module, configuration);
        }

        public Builder with(String module, String reference) {
            return set(module, reference);
        }

        public ModuleConfiguration build() {
            return new ModuleConfiguration(opts);
        }
    }

    public Map<String, ?> toMap() {
        return opts.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            Object value = entry.getValue();
                            if (value instanceof ModuleConfiguration) {
                                return ((ModuleConfiguration) value).toMap();
                            }
                            else {
                                return value;
                            }
                        }));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModuleConfiguration that = (ModuleConfiguration) o;
        return opts.equals(that.opts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opts);
    }
}
