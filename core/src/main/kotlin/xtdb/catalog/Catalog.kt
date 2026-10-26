package xtdb.catalog

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import xtdb.trie.TrieCatalog

/**
 * What one database partition knows about its tables and their tries, as one value behind one pointer.
 *
 * [tableCatalog] and [trieCatalog] read and write through this pointer, so a write to either swaps the whole
 * value, and a reader taking [current] once sees one generation of both halves.
 */
class Catalog(initialState: State) {

    // Not a data class: the pointer's compare-and-set compares by `equals`, which here would walk every
    // table's tries.
    class State(val trieCat: TrieCatalog.State, val tableCat: TableCatalog.State) {

        internal fun mapTableCat(f: (TableCatalog.State) -> TableCatalog.State): State =
            f(tableCat).let { if (it === tableCat) this else State(trieCat, it) }

        internal fun mapTrieCat(f: (TrieCatalog.State) -> TrieCatalog.State): State =
            f(trieCat).let { if (it === trieCat) this else State(it, tableCat) }
    }

    private val stateFlow = MutableStateFlow(initialState)

    val current: State get() = stateFlow.value

    internal val flow: StateFlow<State> get() = stateFlow

    /** [f] MUST be pure — a contended update runs it more than once. */
    internal fun update(f: (State) -> State) = stateFlow.update(f)

    val tableCatalog = TableCatalog(this)
    val trieCatalog = TrieCatalog(this)
}
