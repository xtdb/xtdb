package xtdb.database

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.SourceMessage
import xtdb.database.proto.DatabaseConfig
import xtdb.indexer.LeaderLogProcessor
import xtdb.indexer.LogProcessor.LeaderProcessor
import java.util.*
import kotlin.coroutines.CoroutineContext
import com.google.protobuf.Any as ProtoAny

typealias ExternalSourceToken = ProtoAny

interface ExternalLog<M> : AutoCloseable {

    suspend fun tailAll(afterToken: ExternalSourceToken?, processor: MessageProcessor<M>)

    fun interface MessageProcessor<M> {
        suspend fun processMessages(msgs: List<M>)
    }

    interface Factory {
        fun writeTo(dbConfig: DatabaseConfig.Builder)
        fun open(dbName: String, clusters: Map<LogClusterAlias, Log.Cluster>): ExternalLog<*>
        fun openProcessor(llp: LeaderLogProcessor, dbState: DatabaseState): MessageProcessor<*>

        companion object {
            private val registrations = ServiceLoader.load(Registration::class.java).toList()
            private val registrationsByTag = registrations.associateBy { it.protoTag }

            val serializersModule = SerializersModule {
                for (reg in registrations)
                    include(reg.serializersModule)

                polymorphic(Factory::class) {
                    for (reg in registrations)
                        reg.registerSerde(this)
                }
            }

            fun fromProto(dbConfig: DatabaseConfig): Factory? {
                if (!dbConfig.hasExternalLog()) return null
                val any = dbConfig.externalLog
                val reg = registrationsByTag[any.typeUrl] ?: error("unknown external source: ${any.typeUrl}")
                return reg.fromProto(any)
            }
        }
    }

    interface Registration {
        val protoTag: String
        fun fromProto(msg: ProtoAny): Factory
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }

    class Demux<M> @JvmOverloads constructor(
        private val leaderLogProcessor: LeaderLogProcessor,
        externalLog: ExternalLog<M>, externalSourceToken: ExternalSourceToken?,
        private val externalProc: MessageProcessor<M>,
        ctx: CoroutineContext = Dispatchers.Default
    ) : LeaderProcessor by leaderLogProcessor {

        private val lock = Mutex()

        private val scope = CoroutineScope(ctx)

        private val job = scope.launch {
            try {
                externalLog.tailAll(externalSourceToken) { msgs ->
                    lock.withLock { externalProc.processMessages(msgs) }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                leaderLogProcessor.notifyError(e)
            }
        }

        override suspend fun processRecords(records: List<Log.Record<SourceMessage>>) {
            lock.withLock { leaderLogProcessor.processRecords(records) }
        }

        override fun close() {
            job.cancel()
        }
    }
}
