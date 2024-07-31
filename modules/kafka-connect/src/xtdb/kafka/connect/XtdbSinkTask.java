package xtdb.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.lang.AutoCloseable;
import java.lang.Exception;
import java.util.Collection;
import java.util.Map;


public class XtdbSinkTask extends SinkTask {
    private Map<String,String> props;
    private AutoCloseable client;
    private static IFn submitSinkRecords;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("xtdb.kafka.connect"));
        submitSinkRecords = Clojure.var("xtdb.kafka.connect/submit-sink-records");
    }

    @Override
    public String version() {
        return new XtdbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        this.client = (AutoCloseable) Clojure.var("xtdb.client/start-client").invoke(props.get(XtdbSinkConnector.URL_CONFIG));
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        submitSinkRecords.invoke(client, props, sinkRecords);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        if (client != null)
            try {
                client.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }
}
