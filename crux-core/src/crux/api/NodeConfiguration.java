package crux.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public final class NodeConfiguration {
    static final NodeConfiguration EMPTY = new NodeConfiguration(new HashMap<>());
    private final Map<String, ModuleConfiguration> modules;

    public static NodeConfiguration buildNode(Consumer<Builder> f) {
        Builder builder = new Builder();
        f.accept(builder);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private NodeConfiguration(Map<String, ModuleConfiguration> modules) {
        this.modules = modules;
    }

    public ModuleConfiguration getModule(String key) {
        return modules.get(key);
    }

    public final static class Builder {
        private final Map<String, ModuleConfiguration> modules = new HashMap<>();

        private Builder() {};

        public Builder with(String name, ModuleConfiguration module) {
            modules.put(name, module);
            return this;
        }

        public Builder with(String name) {
            return with(name, ModuleConfiguration.EMPTY);
        }

        public Builder with(String name, Consumer<ModuleConfiguration.Builder> f) {
            modules.put(name, ModuleConfiguration.buildModule(f));
            return this;
        }

        public NodeConfiguration build() {
            return new NodeConfiguration(modules);
        }
    }

    public Map<String, ?> toMap() {
        return modules.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        c -> c.getValue().toMap()));
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
