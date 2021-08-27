package xtdb.kafka.connect;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import xtdb.api.Crux;
import xtdb.api.ICruxAPI;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class XtdbSourceTask extends SourceTask {
    private Map<String,String> props;
    private Closeable api;
    private Map<String,?> sourceOffset;

    private static IFn pollSourceRecords;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("xtdb.kafka.connect"));
        pollSourceRecords = Clojure.var("xtdb.kafka.connect/poll-source-records");
    }

    @Override
    public String version() {
        return new XtdbSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        this.api = (Closeable) Clojure.var("xtdb.api/new-api-client").invoke(props.get(XtdbSinkConnector.URL_CONFIG));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SourceRecord> poll() throws InterruptedException {
        if (sourceOffset == null) {
            sourceOffset = context.offsetStorageReader().offset(Collections.singletonMap(XtdbSourceConnector.URL_CONFIG,
                                                                                         props.get(XtdbSourceConnector.URL_CONFIG)));
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
