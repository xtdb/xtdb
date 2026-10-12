package xtdb.trie

/** What an event in a trie's data page does to its entity — one leg of the page's `op` union. */
enum class EventOp {
    PUT, DELETE, ERASE;

    companion object {
        fun ofLegOrNull(legName: String): EventOp? = when (legName) {
            "put" -> PUT
            "delete" -> DELETE
            "erase" -> ERASE
            else -> null
        }
    }
}
