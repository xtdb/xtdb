package crux.api.alphav2;

import crux.api.Crux;
import crux.api.ICruxAPI;

import java.io.IOException;


public class ExampleUsage {
    static void main(String... args) {
        javaBuilderStyle();
        usingConsumersNested();
        usingConsumersIndividually();
    }

    static void javaBuilderStyle() {
        ModuleConfiguration baz = new ModuleConfiguration.Builder().set("key", "baz").module("waka").build();
        ModuleConfiguration bar = new ModuleConfiguration.Builder().set("key", "bar").with("foo", baz).build();
        NodeConfiguration foo = new NodeConfiguration.Builder().with("key", bar).build();

        try (ICruxAPI node = Crux.startNode(foo)) {
            System.out.println("Used an old Java Builder style");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void usingConsumersNested() {
        try (ICruxAPI node = Crux.startNode(foo -> {
            foo.with("key", bar -> {
               bar.set("key", "bar");
               bar.with("foo", baz -> {
                  baz.set("key", "baz");
                  baz.module("waka");
               });
            });
        })) {
            System.out.println("Used a nested consumer style");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void usingConsumersIndividually() {
        ModuleConfiguration baz = ModuleConfiguration.build( b -> {
           b.set("key", "baz");
           b.module("waka");
        });

        ModuleConfiguration bar = ModuleConfiguration.build( b -> {
            b.set("key", "baz");
            b.with("foo", baz);
        });

        NodeConfiguration foo = NodeConfiguration.build( n -> {
            n.with("key", bar);
        });

        try (ICruxAPI node = Crux.startNode(foo)) {
            System.out.println("Used consumers individually");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
