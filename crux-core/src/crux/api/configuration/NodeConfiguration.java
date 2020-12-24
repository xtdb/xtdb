package crux.api.configuration;

import clojure.lang.PersistentArrayMap;
import crux.api.IBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class NodeConfiguration {
    public static NodeConfiguration configureNode(Consumer<Builder> f) {
        Builder b = new Builder();
        f.accept(b);
        return b.build();
    }

    public static class Builder implements IBuilder<NodeConfiguration> {
        public Builder() {}

        private final HashMap<String, Object> modules = new HashMap<>();

        public Builder with(String module, ModuleConfiguration configuration) {
            modules.put(module, configuration.getOpts());
            return this;
        }

        @Override
        public NodeConfiguration build() {
            return new NodeConfiguration(modules);
        }
    }

    final private Map<String, Object> modules;

    private NodeConfiguration(Map<String, Object> modules) {
        this.modules = modules;
    }

    public Map<?, ?> getModules() {
        return (Map<?, ?>) PersistentArrayMap.create(modules);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeConfiguration that = (NodeConfiguration) o;
        return modules.equals(that.modules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modules);
    }
}