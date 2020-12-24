package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;
import crux.api.configuration.ModuleConfiguration;
import crux.api.configuration.NodeConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.function.Consumer;

import static crux.api.configuration.ModuleConfiguration.configureModule;
import static crux.api.configuration.NodeConfiguration.configureNode;

public class ConfigurationTest {
    @Test
    public void moduleConfigurationBuilder() {
        //Deliberately switch up the orders as it should be irrelevant (assuming no overwriting)
        ModuleConfiguration fromBuilder = new ModuleConfiguration.Builder()
                .module("waka")
                .with("foo")
                .with("bar", "baz")
                .build();

        Consumer<ModuleConfiguration.Builder> consumer = m -> {
            m.with("foo");
            m.with("bar", "baz");
            m.module("waka");
        };

        ModuleConfiguration fromExplicitConsumer = configureModule(consumer);

        ModuleConfiguration fromImplicitConsumer = configureModule ( m -> {
            m.with("foo");
            m.module("waka");
            m.with("bar", "baz");
        });

        HashMap<String, Object> explicitHashMap = new HashMap<>();
        explicitHashMap.put("foo", PersistentArrayMap.EMPTY);
        explicitHashMap.put("crux/module", "waka");
        explicitHashMap.put("bar", "baz");
        IPersistentMap explicitMap = PersistentArrayMap.create(explicitHashMap);

        Assert.assertEquals(fromBuilder, fromExplicitConsumer);
        Assert.assertEquals(fromExplicitConsumer, fromImplicitConsumer);
        Assert.assertEquals(fromImplicitConsumer.getOpts(), explicitMap);
    }

    @Test
    public void nodeConfigurationBuilder() {
        ModuleConfiguration foo = new ModuleConfiguration.Builder()
                .with("foo", "bar")
                .with("baz")
                .module("waka")
                .build();

        Consumer<ModuleConfiguration.Builder> fooConsumer = mc -> {
            mc.with("foo", "bar");
            mc.with("baz");
            mc.module("waka");
        };


        NodeConfiguration fromBuilder = new NodeConfiguration.Builder()
                .with("foo", foo)
                .build();

        Consumer<NodeConfiguration.Builder> consumer = nc -> {
            nc.with("foo", foo);
        };

        NodeConfiguration fromExplicitConsumer = configureNode(consumer);

        NodeConfiguration fromImplicitConsumer = configureNode(nc -> {
            nc.with("foo", configureModule(fooConsumer));
        });

        HashMap<String, Object> explicitSubHashMap = new HashMap<>();
        explicitSubHashMap.put("foo", "bar");
        explicitSubHashMap.put("baz", PersistentArrayMap.EMPTY);
        explicitSubHashMap.put("crux/module", "waka");
        IPersistentMap explicitSubMap = PersistentArrayMap.create(explicitSubHashMap);

        HashMap<String, Object> explicitHashMap = new HashMap<>();
        explicitHashMap.put("foo", explicitSubMap);
        IPersistentMap explicitMap = PersistentArrayMap.create(explicitHashMap);

        Assert.assertEquals(fromBuilder, fromExplicitConsumer);
        Assert.assertEquals(fromBuilder, fromImplicitConsumer);
        Assert.assertEquals(fromBuilder.getModules(), explicitMap);
    }
}