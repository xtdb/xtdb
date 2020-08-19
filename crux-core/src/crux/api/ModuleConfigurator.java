package crux.api;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@SuppressWarnings("unused") // entry points
public class ModuleConfigurator {
    final Map<String, Object> opts = new HashMap<>();

    ModuleConfigurator() {
    }

    public ModuleConfigurator module(String module) {
        opts.put("crux/module", module);
        return this;
    }

    public ModuleConfigurator set(String key, Object value) {
        opts.put(key, value);
        return this;
    }

    public ModuleConfigurator set(Map<String, Object> options) {
        opts.putAll(options);
        return this;
    }

    public ModuleConfigurator with(String module) {
        with(module, c -> {
        });
        return this;
    }

    public ModuleConfigurator with(String module, String reference) {
        return set(module, reference);
    }

    public ModuleConfigurator with(String module, Consumer<ModuleConfigurator> f) {
        ModuleConfigurator c = new ModuleConfigurator();
        f.accept(c);
        opts.put(module, c.opts);
        return this;
    }
}
