package crux.api.query.context

import clojure.lang.Keyword
import crux.api.query.domain.ProjectionSpec
import crux.api.query.domain.ProjectionSpec.Item
import crux.api.query.domain.ProjectionSpec.Item.*
import crux.api.query.domain.ProjectionSpec.Item.Field.Attributes.Companion.empty
import crux.api.underware.BuilderContext
import crux.api.underware.ComplexBuilderContext

class ProjectionSpecContext private constructor(): ComplexBuilderContext<Item, ProjectionSpec>(::ProjectionSpec) {
    companion object : BuilderContext.Companion<ProjectionSpec, ProjectionSpecContext>(::ProjectionSpecContext)

    operator fun Keyword.unaryPlus() = add(Field(this, empty))

    infix fun Keyword.with(block: ProjectionFieldAttributesContext.() -> Unit) =
        add(Field(this, ProjectionFieldAttributesContext.build(block)))

    fun join(keyword: Keyword, block: ProjectionSpecContext.() -> Unit) =
        add(Join(keyword, build(block)))

    fun joinAll(keyword: Keyword) = add(Join(keyword, ProjectionSpec(listOf(ALL))))
}