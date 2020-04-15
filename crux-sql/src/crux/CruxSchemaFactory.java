package crux.calcite;

import java.util.Map;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import clojure.lang.IFn;

public class CruxSchemaFactory implements SchemaFactory {
    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    private static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    public Schema create (SchemaPlus parentSchema, String name, Map<String, Object> operands) {
        return (Schema) resolve("crux.calcite/create-schema").invoke(parentSchema, name, operands);
    }
}
