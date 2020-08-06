package crux.api;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@SuppressWarnings("unused") // entry points
public class ModuleConfigurator {
    final Map<String, Object> opts = new HashMap<>();

    ModuleConfigurator() {
    }

    public void module(String module) {
        opts.put("crux/module", module);
    }

    public void set(String key, Object value) {
        opts.put(key, value);
    }

    public void set(Map<String, Object> options) {
        opts.putAll(options);
    }

    public void with(String module) {
        with(module, c -> {});
    }

    public void with(String module, Consumer<ModuleConfigurator> f) {
        ModuleConfigurator c = new ModuleConfigurator();
        f.accept(c);
        opts.put(module, c.opts);
    }
}
