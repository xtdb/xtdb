package xtdb.api.storage

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import xtdb.api.storage.Storage.STORAGE_ROOT
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.WRITE
import java.util.*
import java.util.concurrent.CompletableFuture
import com.google.protobuf.Any as ProtoAny

interface ObjectStore : AutoCloseable {

    companion object {
        fun throwMissingKey(k: Path): Nothing = error("Object '$k' doesn't exist")
    }

    interface Factory {
        fun openObjectStore(storageRoot: Path = STORAGE_ROOT): ObjectStore
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
     * Asynchronously returns the given object in a ByteBuffer.
     *
     * If the object doesn't exist, the CompletableFuture completes with an IllegalStateException.
     */
    fun getObject(k: Path): CompletableFuture<ByteBuffer>

    /**
     * Asynchronously writes the object to the given path.
     *
     * If the object doesn't exist, the CompletableFuture completes with an IllegalStateException.
     */
    fun getObject(k: Path, outPath: Path): CompletableFuture<Path> =
        getObject(k).thenApply { buf ->
            FileChannel.open(outPath, CREATE, WRITE).use { it.write(buf) }
            outPath
        }

    /**
     * Stores an object in the object store.
     */
    fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<Unit>

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

    fun copyObject(src: Path, dest: Path): CompletableFuture<Unit>

    /**
     * Deletes the object with the given path from the object store.
     */
    fun deleteIfExists(k: Path): CompletableFuture<Unit>

    override fun close() {
    }
}
