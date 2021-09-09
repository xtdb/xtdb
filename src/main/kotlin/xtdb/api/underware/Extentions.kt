package xtdb.api.underware

import clojure.lang.*

val String.kw: Keyword get() = Keyword.intern(this)
val String.sym: Symbol get() = Symbol.intern(this)
val String.clj: Any get() = RT.readString(this)
val <E> List<E>.pv: PersistentVector get() = PersistentVector.create(this)
val <E> List<E>.pl: PersistentList get() = PersistentList.create(this) as PersistentList
val <K, V> Map<K, V>.pam: IPersistentMap get() = PersistentArrayMap.create(this)
fun <E> List<E>.prefix(element: E) = listOf(element) + this