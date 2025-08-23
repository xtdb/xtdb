package xtdb.kafka.connect

import clojure.java.api.Clojure
import clojure.lang.IFn
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.transforms.Transformation

@Suppress("unused")
class SchemaDrivenXtdbEncoder : Transformation<SinkRecord> {
    companion object {
        private val encodeRecordValueBySchema: IFn

        init {
            Clojure.`var`("clojure.core/require").invoke(Clojure.read("xtdb.kafka.connect.encode"))
            encodeRecordValueBySchema = Clojure.`var`("xtdb.kafka.connect.encode/encode-record-value-by-schema")
        }
    }

    override fun apply(record: SinkRecord?): SinkRecord {
        return encodeRecordValueBySchema(record) as SinkRecord
    }

    override fun close() {}

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun config(): ConfigDef {
        return ConfigDef()
    }
}