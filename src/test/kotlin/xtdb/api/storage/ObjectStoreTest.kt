package xtdb.api.storage

import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID.randomUUID
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteRecursively
import kotlin.io.path.readText
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

abstract class ObjectStoreTest {

    abstract fun openObjectStore(prefix: Path): ObjectStore

    private lateinit var prefix: Path
    protected lateinit var objectStore: ObjectStore

    @BeforeTest
    fun setUp() {
        prefix = "xt-os-test-${randomUUID()}".asPath
        objectStore = openObjectStore(prefix)
    }

    @AfterTest
    fun tearDown() {
        objectStore.close()
    }

    private suspend fun ObjectStore.putString(k: String, v: String) {
        putObject(k.asPath, ByteBuffer.wrap(v.encodeToByteArray())).await()
    }

    private suspend fun ObjectStore.getString(k: String): String =
        getObject(k.asPath).await()
            .let { buffer ->
                val bytes = ByteArray(buffer.remaining())
                buffer.get(bytes)
                bytes.decodeToString()
            }

    protected fun randomByteBuffer(size: Int): ByteBuffer =
        ByteBuffer.wrap(ByteArray(size) { Random.nextInt().toByte() })

    @OptIn(ExperimentalPathApi::class)
    @Test
    fun `test put & delete`() = runTest(timeout = 10.seconds) {
        val aliceData = """{:xt/id :alice, :name "Alice"}"""

        objectStore.putString("alice", aliceData)

        assertFailsWith(IllegalStateException::class) { objectStore.getString("bob") }

        val tempDir = createTempDirectory()
        try {
            val outPath = objectStore.getObject("alice".asPath, tempDir.resolve("alice.edn")).await()
            outPath.readText().also { assertEquals(aliceData, it) }
        } finally {
            tempDir.deleteRecursively()
        }

        objectStore.deleteObject("alice".asPath).await()

        assertFailsWith(IllegalStateException::class) { objectStore.getString("alice") }
    }

    private fun ObjectStore.allObjects() = listAllObjects().map { it.key.toString() }.toSet()
    private fun ObjectStore.objects(dir: Path) = listAllObjects(dir).map { it.key.toString() }.toSet()

    @Test
    fun `test list`() = runTest(timeout = 10.seconds) {
        objectStore.putString("bar/alice", ":alice")
        objectStore.putString("bar/bob", ":bob")
        objectStore.putString("foo/alan", ":alan")
        objectStore.putString("bar/baz/dan", ":dan")
        objectStore.putString("bar/baza/james", ":james")

        assertEquals(
            setOf("bar/alice", "bar/bob", "foo/alan", "bar/baz/dan", "bar/baza/james"),
            objectStore.allObjects(),
            "all-objects lists recursively"
        )

        assertEquals(
            setOf("bar/alice", "bar/bob", "bar/baz/dan", "bar/baza/james"),
            objectStore.objects("bar".asPath),
            "lists recursively"
        )

        assertEquals(
            setOf("bar/baz/dan"),
            objectStore.objects("bar/baz".asPath),
            "doesn't include bar/baza"
        )
    }

    @Test
    fun `test list with prior objects`() = runTest(timeout = 10.seconds) {
        objectStore.putString("alice", "alice")
        objectStore.putString("alan", "alan")

        assertEquals(setOf("alice", "alan"), objectStore.allObjects())

        openObjectStore(prefix).use { newObjectStore ->
            assertEquals(setOf("alice", "alan"), newObjectStore.allObjects())
            assertEquals("alice", newObjectStore.getString("alice"))

            newObjectStore.putString("alex", "alex")
            assertEquals(setOf("alice", "alan", "alex"), newObjectStore.allObjects())
            assertEquals("alex", newObjectStore.getString("alex"))
        }
    }

    @Test
    fun `multiple node list test`() = runTest(timeout = 10.seconds) {
        openObjectStore(prefix).use { objectStore2 ->
            objectStore.putString("alice", "alice")
            objectStore2.putString("bob", "bob")

            assertEquals(setOf("alice", "bob"), objectStore.allObjects())
            assertEquals(setOf("alice", "bob"), objectStore2.allObjects())
        }
    }

}