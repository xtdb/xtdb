package crux.api;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class NodeConfigurator {
    final Map<String, Object> modules = new HashMap<>();

    NodeConfigurator() { }

    @SuppressWarnings("unused")
    public NodeConfigurator with(String module, Consumer<ModuleConfigurator> f) {
        ModuleConfigurator c = new ModuleConfigurator();
        f.accept(c);
        modules.put(module, c.opts);
        return this;
    }

    @SuppressWarnings("unused")
    public NodeConfigurator with(String module) {
        with(module, c -> {});
        return this;
    }
}
