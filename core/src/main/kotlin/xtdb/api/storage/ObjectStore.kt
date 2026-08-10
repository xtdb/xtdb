package xtdb.api.storage

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.*
import java.util.concurrent.CompletableFuture
import com.google.protobuf.Any as ProtoAny

interface ObjectStore : AutoCloseable {

    companion object {
        fun throwMissingKey(k: Path): Nothing = error("Object '$k' doesn't exist")
    }

    interface Factory {
        fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote> = emptyMap()): ObjectStore
        val configProto: ProtoAny

        companion object {
            val objectStores = ServiceLoader.load(Registration::class.java).associateBy { it.protoTag }

            val serializersModule = SerializersModule {
                polymorphic(Factory::class) {
                    for (reg in ServiceLoader.load(Registration::class.java))
                        reg.registerSerde(this)
                }
            }

            fun fromProto(objectStore: ProtoAny) =
                (objectStores[objectStore.typeUrl] ?: error("unknown object store: ${objectStore.typeUrl}"))
                    .fromProto(objectStore)
        }
    }

    interface Registration {
        val protoTag: String
        fun fromProto(msg: ProtoAny): Factory

        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
    }

    data class StoredObject(val key: Path, val size: Long)

    /**
     * Returns the given object in a ByteBuffer.
     *
     * If the object doesn't exist, throws an IllegalStateException.
     */
    suspend fun getObject(k: Path): ByteBuffer

    /**
     * Writes the object to the given path.
     *
     * Replaces any existing file at the given path.
     *
     * If the object doesn't exist, throws an IllegalStateException.
     */
    suspend fun getObject(k: Path, outPath: Path): Path =
        getObject(k).let { buf ->
            FileChannel.open(outPath, CREATE, WRITE, TRUNCATE_EXISTING).use { it.write(buf) }
            outPath
        }

    /**
     * Stores an object in the object store.
     *
     * The provided ByteBuffer must not be modified during the execution of this method.
     */
    suspend fun putObject(k: Path, buf: ByteBuffer)

    /**
     * Recursively lists all objects in the object store.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(): Iterable<StoredObject>

    /**
     * Recursively lists all objects in the object store under the given directory.
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAllObjects(dir: Path): Iterable<StoredObject>

    /**
     * Lists objects under the given directory whose keys are lexicographically after [afterKey].
     *
     * Objects are returned in lexicographic order of their path names.
     */
    fun listAfter(dir: Path, afterKey: Path): Iterable<StoredObject> =
        listAllObjects(dir).filter { it.key > afterKey }

    suspend fun copyObject(src: Path, dest: Path)

    /**
     * Deletes the object with the given path from the object store.
     */
    suspend fun deleteIfExists(k: Path)

    override fun close() {
    }
}
