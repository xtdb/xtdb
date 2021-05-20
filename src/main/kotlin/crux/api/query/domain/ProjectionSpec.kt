package crux.api.query.domain

import clojure.lang.Keyword

data class ProjectionSpec(val items: List<Item>) {

    sealed class Item {
        object all: Item()

        data class Field(val keyword: Keyword, val attributes: Attributes): Item() {
            data class Attributes(val attributes: List<Attribute>) {
                companion object {
                    val empty = Attributes(emptyList())
                }
            }

            sealed class Attribute {
                data class Name(val value: Keyword): Attribute()
                data class Limit(val value: Int): Attribute()
                data class Default(val value: Any): Attribute()
            }
        }
    }
}
