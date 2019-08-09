package crux.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;


public class CruxSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CruxSinkTask.class);

    private String filename;
    private PrintStream outputStream;

    public CruxSinkTask() {
    }

    // for testing
    public CruxSinkTask(PrintStream outputStream) {
        filename = null;
        this.outputStream = outputStream;
    }

    @Override
    public String version() {
        return new CruxSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(CruxSinkConnector.FILE_CONFIG);
        if (filename == null) {
            outputStream = System.out;
        } else {
            try {
                outputStream = new PrintStream(
                    Files.newOutputStream(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    false,
                    StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                throw new ConnectException("Couldn't find or create file '" + filename + "' for CruxSinkTask", e);
            }
        }
    }

    private static IFn transformSinkRecord;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.kafka.connect"));
        transformSinkRecord = Clojure.var("crux.kafka.connect/transform-sink-record");
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            log.trace("Writing line to {}: {}", logFilename(), record.value());
            outputStream.println(transformSinkRecord.invoke(record));
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.trace("Flushing output stream for {}", logFilename());
        outputStream.flush();
    }

    @Override
    public void stop() {
        if (outputStream != null && outputStream != System.out)
            outputStream.close();
    }

    private String logFilename() {
        return filename == null ? "stdout" : filename;
    }
}
