package xtdb.vector

interface IVectorPosition {
    var position: Int

    fun getPositionAndIncrement() = position++

    companion object {
        @JvmOverloads
        @JvmStatic
        fun build(initialPosition: Int = 0): IVectorPosition {
            return object : IVectorPosition {
                override var position: Int = initialPosition
            }
        }
    }
}
