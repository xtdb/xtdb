package xtdb.vector

@Suppress("unused")
interface IMultiVectorRelationFactory {
    fun accept(relIdx: Int, vecIdx: Int)
    fun realize(): RelationReader
}