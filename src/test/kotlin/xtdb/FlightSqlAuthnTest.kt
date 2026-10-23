package xtdb

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcException
import org.apache.arrow.adbc.core.AdbcStatusCode
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.CallInfo
import org.apache.arrow.flight.CallOption
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.flight.FlightCallHeaders
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.FlightClientMiddleware
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.arrow.flight.FlightStatusCode
import org.apache.arrow.flight.HeaderCallOption
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Authenticator.Factory.UserList
import xtdb.api.Authenticator.Method.PASSWORD
import xtdb.api.Authenticator.Method.TRUST
import xtdb.api.Authenticator.MethodRule
import xtdb.api.PasswordHash
import xtdb.api.Xtdb
import java.util.Base64

class FlightSqlAuthnTest {

    private val users = mapOf(
        "xtdb" to PasswordHash.argon2id("root-pw"),
        "alice" to PasswordHash.argon2id("alice-pw"),
        "bob" to PasswordHash.argon2id("bob-pw"),
    )

    private lateinit var al: BufferAllocator
    private val toClose = mutableListOf<AutoCloseable>()
    private var port = -1

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        toClose.reversed().forEach { it.close() }
        al.close()
    }

    private fun <T : AutoCloseable> T.tracked(): T = also { toClose.add(it) }

    private fun openNode(rules: List<MethodRule> = listOf(MethodRule(PASSWORD))) {
        val node = Xtdb.openNode {
            authn(UserList(users, rules))
            server { port = 0 }
            flightSql { port = 0 }
        }.tracked()
        port = node.flightSqlPort
    }

    private fun adbc(user: String, password: String): AdbcConnection =
        FlightSqlDriver(al)
            .open(mapOf("uri" to "grpc+tcp://127.0.0.1:$port", "username" to user, "password" to password))
            .tracked()
            .connect().tracked()

    private fun AdbcConnection.update(sql: String) {
        createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.executeUpdate()
        }
    }

    private fun AdbcConnection.rowCount(sql: String): Int =
        createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.executeQuery().use { res ->
                var n = 0
                while (res.reader.loadNextBatch()) n += res.reader.vectorSchemaRoot.rowCount
                n
            }
        }

    private fun basic(user: String, password: String) =
        "Basic " + Base64.getEncoder().encodeToString("$user:$password".toByteArray())

    private fun headers(vararg kvs: Pair<String, String>): CallOption =
        HeaderCallOption(FlightCallHeaders().also { h -> kvs.forEach { (k, v) -> h.insert(k, v) } })

    private class ReturnedAuthorization : FlightClientMiddleware.Factory {
        val values = mutableListOf<String>()

        override fun onCallStarted(info: CallInfo) = object : FlightClientMiddleware {
            override fun onBeforeSendingHeaders(outgoingHeaders: CallHeaders) = Unit
            override fun onHeadersReceived(incomingHeaders: CallHeaders) {
                incomingHeaders.get("authorization")?.let { values.add(it) }
            }
            override fun onCallCompleted(status: CallStatus) = Unit
        }
    }

    private fun client(middleware: FlightClientMiddleware.Factory? = null): FlightSqlClient =
        FlightSqlClient(
            FlightClient.builder(al, Location.forGrpcInsecure("127.0.0.1", port))
                .also { b -> middleware?.let { b.intercept(it) } }
                .build().tracked()
        )

    private fun login(user: String, password: String): String {
        val returned = ReturnedAuthorization()
        client(returned).execute("SELECT 1 AS x", headers("authorization" to basic(user, password)))
        return returned.values.single().removePrefix("Bearer ")
    }

    private fun assertUnauthenticated(f: () -> Unit) =
        assertEquals(FlightStatusCode.UNAUTHENTICATED, assertThrows<FlightRuntimeException> { f() }.status().code())

    @Test
    fun `an ADBC client authenticates with its username and password`() {
        openNode()
        assertEquals(1, adbc("alice", "alice-pw").rowCount("SELECT 1 AS x"))
    }

    @Test
    fun `a wrong password is rejected`() {
        openNode()
        val ex = assertThrows<AdbcException> { adbc("alice", "wrong").rowCount("SELECT 1 AS x") }
        assertEquals(AdbcStatusCode.UNAUTHENTICATED, ex.status)
    }

    @Test
    fun `a client with no credentials is rejected where the rules ask for a password`() {
        openNode()
        assertUnauthenticated { client().execute("SELECT 1 AS x") }
    }

    @Test
    fun `a Flight SQL session's writes run as the user it authenticated as`() {
        openNode()
        adbc("xtdb", "root-pw").update("GRANT analyst TO alice")

        val ex = assertThrows<AdbcException> { adbc("alice", "alice-pw").update("GRANT analyst TO bob") }
        assertTrue("Only a superuser" in ex.message!!, ex.message)
    }

    @Test
    fun `a ticket is redeemed only by an authenticated caller`() {
        openNode()
        val alice = headers("authorization" to "Bearer ${login("alice", "alice-pw")}")
        val ticket = client().execute("SELECT 1 AS x", alice).endpoints.first().ticket

        assertUnauthenticated { client().getStream(ticket).use { it.next() } }
        client().getStream(ticket, alice).use { assertTrue(it.next()) }
    }

    @Test
    fun `where the rules ask for TRUST, a client is taken at its word as the user it names`() {
        openNode(listOf(MethodRule(TRUST)))
        adbc("xtdb", "not-the-password").update("GRANT analyst TO alice")

        val ex = assertThrows<AdbcException> { adbc("alice", "anything").update("GRANT analyst TO bob") }
        assertTrue("Only a superuser" in ex.message!!, ex.message)
    }

    @Test
    fun `a bearer token naming no session is rejected`() {
        openNode()
        assertUnauthenticated { client().execute("SELECT 1 AS x", headers("authorization" to "Bearer not-a-session")) }
    }

    @Test
    fun `a session cookie and bearer token naming different sessions are rejected`() {
        openNode()
        val alice = login("alice", "alice-pw")
        val bob = login("bob", "bob-pw")

        assertUnauthenticated {
            client().execute(
                "SELECT 1 AS x",
                headers("authorization" to "Bearer $bob", "cookie" to "arrow_flight_session_id=$alice")
            )
        }
    }

    @Test
    fun `a session accepts its own credentials again, and no one else's`() {
        openNode()
        val alice = login("alice", "alice-pw")
        val cookie = "cookie" to "arrow_flight_session_id=$alice"

        client().execute("SELECT 1 AS x", headers(cookie, "authorization" to basic("alice", "alice-pw")))

        assertUnauthenticated {
            client().execute("SELECT 1 AS x", headers(cookie, "authorization" to basic("bob", "bob-pw")))
        }
    }

    @Test
    fun `rules see the caller's own address, whatever address header it sends`() {
        openNode(listOf(MethodRule(TRUST, remoteAddress = "127.0.0.1"), MethodRule(PASSWORD)))
        client().execute("SELECT 1 AS x")

        openNode(listOf(MethodRule(TRUST, remoteAddress = "10.0.0.5"), MethodRule(PASSWORD)))
        assertUnauthenticated { client().execute("SELECT 1 AS x", headers("x-xtdb-peer-addr" to "10.0.0.5")) }
    }
}
