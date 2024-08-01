package xtdb.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class XtdbSinkConnector extends SinkConnector {

    public static final String URL_CONFIG = "url";
    public static final String ID_MODE_CONFIG = "id.mode";
    public static final String ID_FIELD_CONFIG = "id.field";
    public static final String VALID_FROM_FIELD_CONFIG = "validFrom.field";
    public static final String VALID_TO_FIELD_CONFIG = "validTo.field";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(URL_CONFIG,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                "Destination URL of XTDB HTTP end point.")
        .define(ID_MODE_CONFIG,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                EnumValidator.of("record_key", "record_value"),
                Importance.HIGH,
                "The id mode. Supported modes are `record_key` and `record_value`.")
        .define(ID_FIELD_CONFIG,
                Type.STRING,
                "",
                Importance.MEDIUM,
                "The field name to use as _id or empty if using a primitive `record_key`.")
        .define(VALID_FROM_FIELD_CONFIG,
                Type.STRING,
                "",
                Importance.LOW,
                "The field name to use as _valid_from. Leave empty to use the default _valid_from.")
        .define(VALID_TO_FIELD_CONFIG,
                Type.STRING,
                "",
                Importance.LOW,
                "The field name to use as _valid_to. Leave empty to use the default _valid_to.");

    private String url;
    private String idMode;
    private String idField;
    private String validFromField;
    private String validToField;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        url = parsedConfig.getString(URL_CONFIG);
        idMode = parsedConfig.getString(ID_MODE_CONFIG);
        idField = parsedConfig.getString(ID_FIELD_CONFIG);
        validFromField = parsedConfig.getString(VALID_FROM_FIELD_CONFIG);
        validToField = parsedConfig.getString(VALID_TO_FIELD_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return XtdbSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(URL_CONFIG, url);
            config.put(ID_MODE_CONFIG, idMode);
            config.put(ID_FIELD_CONFIG, idField);
            config.put(VALID_FROM_FIELD_CONFIG, validFromField);
            config.put(VALID_TO_FIELD_CONFIG, validToField);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private static class EnumValidator implements ConfigDef.Validator {
        private final Set<String> validValues;

        private EnumValidator(Set<String> validValues) {
            this.validValues = validValues;
        }

        public static EnumValidator of(String... values) {
            return new EnumValidator(Set.of(values));
        }

        @Override
        public void ensureValid(String key, Object value) {
            if (value != null && !validValues.contains(value)) {
              throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return validValues.toString();
        }
    }
}
