package xtdb.kafka.connect

import clojure.java.api.Clojure
import clojure.lang.IFn
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

class XtdbSinkTask : SinkTask() {
    companion object {
        private val submitSinkRecords: IFn

        init {
            Clojure.`var`("clojure.core/require").invoke(Clojure.read("xtdb.kafka.connect"))
            submitSinkRecords = Clojure.`var`("xtdb.kafka.connect/submit-sink-records")
        }
    }

    private lateinit var config: XtdbSinkConfig

    override fun start(props: Map<String, String>) {
        config = XtdbSinkConfig.parse(props)
    }

    override fun put(sinkRecords: Collection<SinkRecord>) {
        submitSinkRecords(config.connectionUrl, config, sinkRecords)
    }

    override fun version(): String = XtdbSinkConnector().version()

    override fun flush(offsets: Map<TopicPartition, OffsetAndMetadata>) {
    }

    override fun stop() {
    }
}
