package crux.api.query.context

import clojure.lang.Keyword
import crux.api.query.domain.ProjectionSpec.Item.Field.Attribute
import crux.api.query.domain.ProjectionSpec.Item.Field.Attribute.*
import crux.api.query.domain.ProjectionSpec.Item.Field.Attributes
import crux.api.underware.BuilderContext
import crux.api.underware.SimpleBuilderContext
import javax.naming.OperationNotSupportedException

class ProjectionFieldAttributesContext: SimpleBuilderContext<Attribute, Attributes>(::Attributes) {
    companion object : BuilderContext.Companion<Attributes, ProjectionFieldAttributesContext>(::ProjectionFieldAttributesContext)

    var name: Keyword
        get() = throw OperationNotSupportedException()
        set(value) = add(Name(value))

    var limit: Int
        get() = throw OperationNotSupportedException()
        set(value) = add(Limit(value))

    var default: Any
        get() = throw OperationNotSupportedException()
        set(value) = add(Default(value))
}