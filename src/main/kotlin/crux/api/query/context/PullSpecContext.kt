package crux.api.query.context

import clojure.lang.Keyword
import crux.api.query.domain.PullSpec
import crux.api.query.domain.PullSpec.Item
import crux.api.query.domain.PullSpec.Item.*
import crux.api.query.domain.PullSpec.Item.Field.Attributes.Companion.empty
import crux.api.underware.BuilderContext
import crux.api.underware.ComplexBuilderContext

class PullSpecContext private constructor(): ComplexBuilderContext<Item, PullSpec>(::PullSpec) {
    companion object : BuilderContext.Companion<PullSpec, PullSpecContext>(::PullSpecContext)

    operator fun Keyword.unaryPlus() = add(Field(this, empty))

    infix fun Keyword.with(block: PullFieldAttributesContext.() -> Unit) =
        add(Field(this, PullFieldAttributesContext.build(block)))

    fun join(keyword: Keyword, block: PullSpecContext.() -> Unit) =
        add(Join(keyword, build(block)))

    fun joinAll(keyword: Keyword) = add(Join(keyword, PullSpec(listOf(ALL))))
}