package crux.api.alphav2;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class NodeConfiguration {
    public static NodeConfiguration build(Consumer<Builder> f) {
        Builder b = new Builder();
        f.accept(b);
        return b.build();
    }

    public static class Builder implements IBuilder<NodeConfiguration> {
        public Builder() {}

        private final HashMap<String, Object> modules = new HashMap<>();

        public Builder with(String module, ModuleConfiguration configuration) {
            modules.put(module, configuration);
            return this;
        }

        public Builder with(String module, Consumer<ModuleConfiguration.Builder> f) {
            modules.put(module, ModuleConfiguration.build(f));
            return this;
        }

        @Override
        public NodeConfiguration build() {
            return new NodeConfiguration(modules);
        }
    }

    final private IPersistentMap modules;

    private NodeConfiguration(Map<String, Object> modules) {
        this.modules = PersistentArrayMap.create(modules);
    }

    public IPersistentMap getModules() {
        return modules;
    }
}
