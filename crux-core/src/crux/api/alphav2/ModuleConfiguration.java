package crux.api.alphav2;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ModuleConfiguration {
    public static ModuleConfiguration build(Consumer<Builder> f) {
        ModuleConfiguration.Builder b = new ModuleConfiguration.Builder();
        f.accept(b);
        return b.build();
    }

    static public class Builder implements IBuilder<ModuleConfiguration> {
        final Map<String, Object> opts = new HashMap<>();

        public Builder module(String module) {
            opts.put("crux/module", module);
            return this;
        }

        public Builder set(String key, Object value) {
            opts.put(key, value);
            return this;
        }

        public Builder set(Map<String, Object> options) {
            opts.putAll(options);
            return this;
        }

        public Builder with(String module) {
            return with(module, c -> {});
        }

        public Builder with(String module, String reference) {
            return set(module, reference);
        }

        public Builder with(String module, Consumer<ModuleConfiguration.Builder> f) {
            return with(module, ModuleConfiguration.build(f));
        }

        public Builder with(String module, ModuleConfiguration config) {
            opts.put(module, config.opts);
            return this;
        }

        @Override
        public ModuleConfiguration build() {
            return new ModuleConfiguration(opts);
        }
    }

    private final IPersistentMap opts;

    private ModuleConfiguration(Map<String, Object> opts) {
        this.opts = PersistentArrayMap.create(opts);
    }

    public IPersistentMap getOpts() {
        return opts;
    }
}
