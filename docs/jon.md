Jon Questions

Is binary-index / compound index a thing?

Let me attempt to paraphrase - to prove my understanding. We have
`attribute+value+entity+content-hash-index` - also known as `VE`,
which is a physical index on disk. Happy days. The reason that `VE` is
a binary index, is because there's 2 parts to it, `V` and `E`,
although one might immediately complain that there's 3 - `A`, `V`, and
`E`, but let's park that. So we have `VE`. If `VE` were a single
index, then `next-values` simply hops us around the 'E' part,
occasionally jumping a `V` boundary. To get some more power and perf,
we need a index sitting higher up, so that `next-values` jumps us
around the `V`s. (edited)

We have 2 levels of binary indexes: first is what I described, a
binary index covering both individual indexes V and E in the case of
VE. Then we have a binary index sitting higher up, covering both VE
and EV. Hence we get our tree The top one is a 'crux.db.LayeredIndex'

@jon your first sentence is totally correct up until your very valid
complaint about the third element, the attribute there's a comment
somewhere mentioning that we could have ternary indexes if we wanted
to support variables in attribute position, but this increases the
amount of indexes needed (to 6 I think) so `VE`is an index of values
for a specific A, like `:name`, so if V is `"Fred"` next will give
`"Ivan"` for example as long as the binary index doesn't have
`open-level` called, which means it will move on to Es for a specific
V in which case `seek/next-values` will operate on the entities for a
specific V, like `"Fred"` so you might get `:fred1`
`:fredrick-the-great` etc.  but you won't get `:ivan` (or
`:ivan-the-terrible` to continue the theme) without first doing
`close-level` and a `next-values` on the V level (moving to `"Fred"`),
followed by another `open-level` to enter the domain of `"Ivan"`
(edited)

Let's take an example entity-attribute-value query tuple of `[x :name
y]`. This is turned into a join attempt using the `AVE`
`LayeredIndex`, where `y` will bind to `V` layer, and `x` will bind to
the `E` layer.

Using the LayeredIndex `AVE`, the join will start on `y`, looking for
`V`s for the attribute `name`. Each time a `V` is found, `open-level`
is called, and we will capture all `E`s that sit underneath the `V`,
the `AVE` index.

The order of operations against the layered index AVE, goes as such:
`seek-values nil, y = "Ivan", open-level, seek-values nil, x = :ivan,
next-values, x = :ivan-the-terrible` and as there are no more `E`s
here, it goes `close-level, next-values, y = "Fred", open-level
seek-values nil, x = :fred, next-values :fredrick-the-great` etc.
`nil` here will be the case if there's only one index, in which case
it will scan each index from scratch if there's more than one index
(usually the case) joining against each other, they will constrain
each others values (this is the leapfrog part, the part moving up and
down the tree is the triejoin) but here we're not currently discussing
the actual join at for a single var, that's kind of orthogonal to
understanding how the layered indexes work I've updated the tests a
bit (they don't use binary indexes, but in memory relations, which are
similar tuples) so if you read `crux.doc-test` for the join tests it
might also clear it up a bit




Our `LayeredIndex`

Hakan Raberg [11:34 AM] so the binary index wraps the VE-V and VE-E
indexes where we first join on V and then E, each being a level in the
n-ary join (that is, the query itself) so if the query is  VE-V will bind `y` and VE-E bind `x` and the join order (to keep
the case we've chosen) is `[y x]` (edited) so the join starts on `y`,
finds valid ys, in this case in the VE-V index, and once it has one,
the join moves on to `x` and calls `open-level` on all x indexes -
which in this case is -the same- binary index, which on this command
moves on to the VE-E index internally so it goes `seek-values nil, y =
"Ivan", open-level, seek-values nil, x = :ivan, next-values, x =
:ivan-the-terrible` and as there are no more es here, it goes
`close-level, next-values, y = "Fred", open-level seek-values nil, x =
:fred, next-values :fredrick-the-great` etc.  `nil` here will be the
case if there's only one index, in which case it will scan each index
from scratch if there's more than one index (usually the case) joining
against each other, they will constrain each others values (this is
the leapfrog part, the part moving up and down the tree is the
triejoin) but here we're not currently discussing the actual join at
for a single var, that's kind of orthogonal to understanding how the
layered indexes work I've updated the tests a bit (they don't use
binary indexes, but in memory relations, which are similar tuples) so
if you read `crux.doc-test` for the join tests it might also clear it
up a bit

Jon Pither [11:44 AM]
This is v helpful

Hakan Raberg [11:45 AM]
to be clear, in the example above, every time you manage to reach a leaf you have a result, so the result in this example is `#{["Ivan" :ivan] ["Ivan" :ivan-the-terrible] ["Fred" :fred ] ["Fred" :fredrick-the-great]}`
you might not reach a leaf if there's no common values when you join indexes, or if a constraining fn (like a predicate) rejects a branch of the tree as its walked
so this is also how the laziness works, as the tree is "just walked" you can generate a tuple lazily every time you reach a leaf, turning it into a set at the end is just a API niceness, there's another arity of the `q` function where you provide an opened snapshot (from which iterators are opened) and manage that yourself, allowing you to treat the resulting seq of tuples like any other lazy seq
the tree itself is always ordered at every level, when the user requests a different order via `:order-by` it has to be re sorted - laziness is preserved, and the sort will spill over to disk if the result is big (edited)
(but this is obviously much slower)
as I mentioned the other day, it is in theory possible to pick a join order that agrees with the requested sort order, but there are some gotchas there, but it can in theory be done
in that case you don't have to resort, and get a lazy stream in your requested sort order directly
but this doesn't exist yet, as it is hard
further trivia, so the constraining functions are applied at different join depths, so say you have a function like `[(re-find #"I" y)]` and the order is `[y x]` this will fire as each y is bound, though currently things are only bound when both the e and v vars in a pair are bound (as the binary index need to reach the content hashes to see if its a value version) (edited)
further trivia, if the predicate is a function, like `[(re-find #"I" y) z]` the join order (in this case) will be `[y x z]` and z will b dynamically updated by the result of the fn each time that level (y, but effectively x due to the pairing) of the tree is walked, inserting new values deeper in the tree (edited)
`or` and rules work the same way, but they can introduce more than one variable
so in this case the zs don't come from the indexes on disk, but from an atom in memory which is updated when the function fires
in the EV indexes, EV-E will prune the tree before reaching EV-V if the E itself is invalid (that is, it doesn't exist or has been deleted)
the VE-V index could in theory not have to wait until VE-E is bound to fire the predicate discussed above, but currently there's no way of reading the values out of the index, the values are always taken from an actual doc, that is, the index is treated as a one way structure, you can write to it, join on it, but cannot decode it
and to get a doc, you need an e, so in the running example, x is looked up, the doc is loaded `{:crux.db/id :ivan :name "Ivan"}` and y is taken from `:name` there (edited)
now obviously, in Datomic, v is taken from the index itself (there are no docs!), and we could consider introducing decoding of the index values to avoid having to lookup the doc in most (all?) cases. (edited)
