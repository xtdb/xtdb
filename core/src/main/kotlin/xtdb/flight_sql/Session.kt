package xtdb.flight_sql

import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.CallInfo
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.flight.FlightServerMiddleware
import org.apache.arrow.flight.RequestContext
import xtdb.api.Xtdb
import xtdb.database.DatabaseName
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicReference

internal const val SESSION_COOKIE = "arrow_flight_session_id"

internal class Session(
    val id: String,
    override val lease: Lease,
    val caller: Caller,
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

    /** A connection of the session's own, as its caller, that the session doesn't share or close — a transaction's. */
    fun openConnection(dbName: DatabaseName): Xtdb.Connection = connect(dbName)

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
            if (session.isClosed) return
            outgoingHeaders.insert("set-cookie", "$SESSION_COOKIE=${session.id}")
            // drivers replay the `authorization` a server returns on every later call, many of them in place of cookies
            if (session.caller is Caller.Authenticated) outgoingHeaders.insert("authorization", "Bearer ${session.id}")
        }
    }

    override fun onCallCompleted(status: CallStatus?) {
        session.lease.release()
    }

    override fun onCallErrored(err: Throwable?) = Unit

    class Factory(
        private val sessions: Leases<String, Session>,
        private val authn: FlightAuthn,
        private val newSession: (Caller) -> Session,
    ) : FlightServerMiddleware.Factory<SessionMiddleware> {

        override fun onCallStarted(
            info: CallInfo, incomingHeaders: CallHeaders, context: RequestContext
        ): SessionMiddleware {
            val credentials = Credentials.parse(incomingHeaders)

            val cookieId = incomingHeaders.getAll("cookie")
                .flatMap { it.split(';') }
                .map { it.trim().split('=', limit = 2) }
                .firstOrNull { it.size == 2 && it[0] == SESSION_COOKIE && it[1].isNotEmpty() }
                ?.get(1)

            val bearerId = (credentials as? Credentials.Bearer)?.sessionId

            if (cookieId != null && bearerId != null
                && !MessageDigest.isEqual(cookieId.toByteArray(), bearerId.toByteArray())
            ) throw unauthenticated("the session cookie and bearer token name different sessions")

            val id = bearerId ?: cookieId
                ?: return Minted(
                    newSession(authn.authenticate(credentials, incomingHeaders.peerAddress))
                        .also { it.lease.acquire(); sessions.add(it.id, it) }
                )

            val session = when (val found = sessions.acquire(id)) {
                is Leases.Lookup.Found -> found.value
                is Leases.Lookup.Expired -> "session expired after ${found.after} idle".let {
                    throw if (bearerId != null) unauthenticated(it) else CallStatus.NOT_FOUND.withDescription(it).toRuntimeException()
                }
                Leases.Lookup.Unknown -> "unknown session".let {
                    throw if (bearerId != null) unauthenticated(it) else CallStatus.NOT_FOUND.withDescription(it).toRuntimeException()
                }
            }

            try {
                if (credentials is Credentials.Verifiable && !session.caller.authenticatedBy(authn.mac(credentials)))
                    throw unauthenticated("these credentials aren't the ones the session was authenticated with")
            } catch (e: Throwable) {
                session.lease.release()
                throw e
            }

            try {
                session.caller.revalidate()
            } catch (e: Throwable) {
                (sessions.take(id) as? Leases.Lookup.Found)?.value?.close()
                throw e
            }

            return Presented(session)
        }
    }
}
