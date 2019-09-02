package crux.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CruxSourceConnector extends SourceConnector {
    public static final String URL_CONFIG = "url";
    public static final String TOPIC_CONFIG = "topic";
    public static final String FORMAT_CONFIG = "format";
    public static final String MODE_CONFIG = "mode";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;
    public static final String DEFAULT_FORMAT = "edn";
    public static final String DEFAULT_MODE = "tx";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(URL_CONFIG, Type.STRING, "http://localhost:3000", Importance.HIGH, "Destination URL of Crux HTTP end point.")
        .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to")
        .define(FORMAT_CONFIG, Type.STRING, DEFAULT_FORMAT, ConfigDef.ValidString.in("edn", "json", "transit"), Importance.LOW, "Format to use, edn, json or transit (json-verbose)")
        .define(MODE_CONFIG, Type.STRING, DEFAULT_MODE, ConfigDef.ValidString.in("tx", "doc"), Importance.LOW, "Mode to use, tx or doc")
        .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
                "The maximum number of records the Source task can read from Crux at one time");

    private String url;
    private String topic;
    private String format;
    private String mode;
    private int batchSize;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        topic = parsedConfig.getString(TOPIC_CONFIG);
        url = parsedConfig.getString(URL_CONFIG);
        format = parsedConfig.getString(FORMAT_CONFIG);
        mode = parsedConfig.getString(MODE_CONFIG);
        batchSize = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CruxSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(URL_CONFIG, url);
        config.put(FORMAT_CONFIG, format);
        config.put(MODE_CONFIG, mode);
        config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
