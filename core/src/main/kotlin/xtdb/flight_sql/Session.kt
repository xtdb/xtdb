package xtdb.flight_sql

import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.CallInfo
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.flight.FlightServerMiddleware
import org.apache.arrow.flight.RequestContext
import xtdb.api.Xtdb
import xtdb.database.DatabaseName
import java.util.concurrent.atomic.AtomicReference

internal const val SESSION_COOKIE = "arrow_flight_session_id"

internal class Session(
    val id: String,
    override val lease: Lease,
    private val connect: (DatabaseName) -> Xtdb.Connection,
) : Leased {

    private sealed interface State {
        data class Open(val conns: Map<DatabaseName, Xtdb.Connection>) : State
        data object Closed : State
    }

    private val state = AtomicReference<State>(State.Open(emptyMap()))

    @Volatile
    var catalog: DatabaseName? = null

    val isClosed: Boolean get() = state.get() == State.Closed

    fun connection(dbName: DatabaseName): Xtdb.Connection {
        while (true) {
            when (val s = state.get()) {
                is State.Open -> {
                    s.conns[dbName]?.let { return it }
                    val conn = connect(dbName)
                    if (state.compareAndSet(s, State.Open(s.conns + (dbName to conn)))) return conn
                    conn.close()
                }

                State.Closed -> throw CallStatus.NOT_FOUND.withDescription("session closed").toRuntimeException()
            }
        }
    }

    override fun close() {
        when (val s = state.getAndSet(State.Closed)) {
            is State.Open -> s.conns.values.forEach { it.close() }
            State.Closed -> Unit
        }
    }
}

internal sealed class SessionMiddleware(val session: Session) : FlightServerMiddleware {

    class Presented(session: Session) : SessionMiddleware(session) {
        override fun onBeforeSendingHeaders(outgoingHeaders: CallHeaders) {
            if (session.isClosed) outgoingHeaders.insert("set-cookie", "$SESSION_COOKIE=${session.id}; Max-Age=0")
        }
    }

    class Minted(session: Session) : SessionMiddleware(session) {
        override fun onBeforeSendingHeaders(outgoingHeaders: CallHeaders) {
            if (!session.isClosed) outgoingHeaders.insert("set-cookie", "$SESSION_COOKIE=${session.id}")
        }
    }

    override fun onCallCompleted(status: CallStatus?) {
        session.lease.release()
    }

    override fun onCallErrored(err: Throwable?) = Unit

    class Factory(
        private val sessions: Leases<String, Session>,
        private val newSession: () -> Session,
    ) : FlightServerMiddleware.Factory<SessionMiddleware> {

        override fun onCallStarted(
            info: CallInfo, incomingHeaders: CallHeaders, context: RequestContext
        ): SessionMiddleware {
            val id = incomingHeaders.getAll("cookie")
                .flatMap { it.split(';') }
                .map { it.trim().split('=', limit = 2) }
                .firstOrNull { it.size == 2 && it[0] == SESSION_COOKIE && it[1].isNotEmpty() }
                ?.get(1)
                ?: return Minted(newSession().also { it.lease.acquire(); sessions.add(it.id, it) })

            return when (val found = sessions.acquire(id)) {
                is Leases.Lookup.Found -> Presented(found.value)
                is Leases.Lookup.Expired ->
                    throw CallStatus.NOT_FOUND.withDescription("session expired after ${found.after} idle").toRuntimeException()
                Leases.Lookup.Unknown -> throw CallStatus.NOT_FOUND.withDescription("unknown session").toRuntimeException()
            }
        }
    }
}
