package xtdb.calcite;

import java.util.Map;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

public class XtdbSchemaFactory implements SchemaFactory {
    public Schema create (SchemaPlus parentSchema, String name, Map<String, Object> operands) {
        return (Schema) XtdbUtils.resolve("xtdb.calcite/create-schema").invoke(parentSchema, name, operands);
    }
}
