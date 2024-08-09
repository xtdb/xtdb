package xtdb.kafka.connect

import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

@Suppress("MemberVisibilityCanBePrivate")
class XtdbSinkConnector : SinkConnector() {

    lateinit var xtConfig: XtdbSinkConfig

    override fun version(): String = AppInfoParser.getVersion()

    override fun taskClass(): Class<out Task> = XtdbSinkTask::class.java

    override fun config() = CONFIG_DEF

    override fun start(props: Map<String, String>) {
        xtConfig = XtdbSinkConfig.parse(props)
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String?>> =
        xtConfig.taskConfig.let { config -> List(maxTasks) { config } }

    override fun stop() {
    }
}
