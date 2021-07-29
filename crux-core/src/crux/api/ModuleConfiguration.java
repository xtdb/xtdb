package crux.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Class to configure a Crux module.
 *
 * See https://opencrux.com/reference/configuration.html for examples.
 */
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

        /**
         * Sets the configuration key to the given value
         * @return this
         */
        public Builder set(String key, Object value) {
            opts.put(key, value);
            return this;
        }

        /**
         * Specifies the constructor of this module.
         *
         * If it is not explicitly provided, Crux will use the key that this module is referenced by.
         * e.g. if it is included under a `"crux.rocksdb/kv-store"` key,
         * Crux will use the `"crux.rocksdb/-&gt;kv-store"` Clojure function to construct the module.
         *
         * @param module the module constructor, e.g. `"crux.rocksdb/-&gt;kv-store"`
         * @return this
         */
        public Builder module(String module) {
            return set(MODULE, module);
        }

        public Builder set(Map<String, Object> options) {
            opts.putAll(options);
            return this;
        }

        /**
         * Adds the submodule to this module with the default parameters
         * @return this
         */
        public Builder with(String module) {
            return set(module, ModuleConfiguration.EMPTY);
        }

        /**
         * Adds the submodule to this module with the given configuration
         * @return this
         */
        public Builder with(String module, ModuleConfiguration configuration) {
            return set(module, configuration);
        }

        /**
         * Adds the submodule to this module with the given configuration
         * @return this
         */
        public Builder with(String name, Consumer<ModuleConfiguration.Builder> f) {
            return with(name, ModuleConfiguration.buildModule(f));
        }

        /**
         * Adds an existing top-level module to this submodule as a dependency.
         *
         * @param key the key under which the dependency will appear to this module
         * @param module the key of the module to refer to
         * @return this
         */
        public Builder with(String key, String module) {
            return set(key, module);
        }

        public ModuleConfiguration build() {
            return new ModuleConfiguration(opts);
        }
    }

    /**
     * Not for public use, may be removed.
    */
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
