package xtdb.arrow

interface VectorPosition {
    var position: Int

    fun getPositionAndIncrement() = position++

    companion object {
        @JvmOverloads
        @JvmStatic
        fun build(initialPosition: Int = 0): VectorPosition {
            return object : VectorPosition {
                override var position: Int = initialPosition
            }
        }
    }
}
