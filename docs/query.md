## Graph Query

### Datascript Compatibility

#### Query Arguments

Datascript queries supply parameters as arguments outside the main query.

For example

````
(query
{:find ['e]
 :in [$name]
 :where [['e :name $name]]}
 "Ivan")
````

As oppose to

````
{:find ['e]
 :where [['e :name "Ivan"]]}

````

If you use parameterised queries, you can of course optimise the queries (saves on parsing and validation).

I'm yet to discover if there is another reason. Crux may want to hold off on a view here, until the technical case is made, or another reason found.
