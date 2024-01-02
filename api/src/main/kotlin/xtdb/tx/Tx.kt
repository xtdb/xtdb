package xtdb.tx

data class Tx(val txOps: List<Ops>, val txOptions: TxOptions) {
    fun txOps(): List<Ops> = txOps
    fun txOptions(): TxOptions = txOptions
}
