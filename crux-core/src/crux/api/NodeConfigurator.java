package crux.api;

import crux.api.configuration.ModuleConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/*
public class NodeConfigurator {
    final Map<String, Object> modules = new HashMap<>();

    NodeConfigurator() { }

    @SuppressWarnings("unused")
    public NodeConfigurator with(String module, ModuleConfiguration configuration) {
        modules.put(module, configuration.getOpts());
        return this;
    }

    @SuppressWarnings("unused")
    public NodeConfigurator with(String module) {
        with(module, ModuleConfiguration.build(mc -> {}));
        return this;
    }
}*/
