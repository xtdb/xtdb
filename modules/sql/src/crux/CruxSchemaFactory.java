package crux.calcite;

import java.util.Map;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

public class CruxSchemaFactory implements SchemaFactory {
    public Schema create (SchemaPlus parentSchema, String name, Map<String, Object> operands) {
        return (Schema) CruxUtils.resolve("crux.calcite/create-schema").invoke(parentSchema, name, operands);
    }
}
