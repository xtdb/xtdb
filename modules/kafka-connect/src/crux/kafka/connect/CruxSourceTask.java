package crux.kafka.connect;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import crux.api.Crux;
import crux.api.ICruxAPI;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class CruxSourceTask extends SourceTask {
    private Map<String,String> props;
    private Closeable api;
    private Map<String,?> sourceOffset;

    private static IFn pollSourceRecords;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.kafka.connect"));
        pollSourceRecords = Clojure.var("crux.kafka.connect/poll-source-records");
    }

    @Override
    public String version() {
        return new CruxSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        this.api = (Closeable) Clojure.var("crux.api/new-api-client").invoke(props.get(CruxSinkConnector.URL_CONFIG));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SourceRecord> poll() throws InterruptedException {
        if (sourceOffset == null) {
            sourceOffset = context.offsetStorageReader().offset(Collections.singletonMap(CruxSourceConnector.URL_CONFIG,
                                                                                         props.get(CruxSourceConnector.URL_CONFIG)));
        }
        List<SourceRecord> records = (List<SourceRecord>) pollSourceRecords.invoke(api, sourceOffset, props);
        if (!records.isEmpty()) {
            sourceOffset = records.get(records.size() - 1).sourceOffset();
        }
        return records;
    }

    @Override
    public void stop() {
        if (api != null)
            try {
                api.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
    }
}
