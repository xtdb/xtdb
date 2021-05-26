package crux.api.query.conversion

import crux.api.query.domain.PullSpec
import crux.api.query.domain.PullSpec.Item
import crux.api.query.domain.PullSpec.Item.Field.Attribute
import crux.api.query.domain.PullSpec.Item.Field.Attributes
import crux.api.underware.*

fun PullSpec.toEdn() = items.map(Item::toEdn).pv

private val ALL = "*".sym

fun Item.toEdn(): Any = when(this) {
    Item.ALL -> ALL
    is Item.Field -> listOf(keyword, attributes.toEdn()).pl
    is Item.Join -> mapOf(keyword to spec.toEdn()).pam
}

fun Attributes.toEdn() = attributes.map(Attribute::toEdn).toMap().pam

private val NAME = "as".kw
private val LIMIT = "limit".kw
private val DEFAULT = "default".kw

fun Attribute.toEdn() = when(this) {
    is Attribute.Name -> NAME to value
    is Attribute.Limit -> LIMIT to value
    is Attribute.Default -> DEFAULT to value
}