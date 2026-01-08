(ns xtdb.arrow-readers-writers-doc-test
  (:require [clojure.test :as t])
  (:import (org.apache.arrow.memory RootAllocator)
           (xtdb.arrow DenseUnionVector Relation Vector)))

(t/deftest arrow-readers-writers-demo

  ;; The namespace is documentation illustrating how readers, writers and copiers work as well as a
  ;; give a pragmatic intro to Arrow.

  ;; Before reading this one should be somehow familiar with the concepts of
  ;; https://arrow.apache.org/docs/format/Columnar.html#format-columnar
  ;; Get familiar with the terminology section and read the start of the physical memory layout.
  ;; You don't need to know every physical memory layout, but maybe look at one or two to get a feel
  ;; for things.

  ;; Our engine works on relations. Think of a relation as a set of vectors where each vector has a name.
  ;; Lets say you submit the following two documents.
  [:put-docs :xt-docs {:xt/id 1 :column-name 42}]
  [:put-docs :xt-docs {:xt/id 2 :column-name "foo"}]
  ;; then this gets transformed into the relation (forgetting the special treatement of `xt/id` for the moment)
  {"xt/id" [1 2]
   "column-name" [42 "foo"]}
  ;; where the keys are the names and the value vectors are actual Vectors.

  ;; If you want to represent a vector with two or more base types you need
  ;; a dense union vector (duv). A duv has legs (other names are members or variants) to different types (not strictly true
  ;; as you could have two legs with a different name but same type). So for a every position of a duv you have one valid
  ;; leg position. In the case of `column-name` above would get a duv with a leg `i64` and `utf8`. The 42 would then sit in first
  ;; position of the `i64` vector and the "foo" value in the first  position of the `utf8` vector.

  ;; Types that are not nested (`i64`,`utf8`, etc...) are called primitive types and nested types are types
  ;; that have children (`:union`, `:list`, `:struct`).

  ;; ArrowType: This is essentially just describing the toplevel type structure. We have a special reader macro
  ;; `#xt.arrow/type` to define it.

  #xt.arrow/type :i64
  ;; => #xt.arrow/type :i64

  ;; We are not talking about children here.
  #xt.arrow/type :union
  ;; => #xt.arrow/type :union

  ;; VectorType combines ArrowType with nullability
  #xt/type :i64
  ;; => <VectorType #xt.arrow/type :i64 not-null>

  #xt/type [:? :union]
  ;; => <VectorType #xt.arrow/type :union null>

  ;; Fields are essentially ArrowType + nullable + naming + potential child Fields
  #xt/field {"my-int" [:? :i64]}
  ;; => <Field "my-int" #xt.arrow/type :i64 null>

  ;; See how we are specifying a child here.
  #xt/field {"my-union" [:union {"my-int" [:? :i64]}]}
  ;; => <Field "my-union" #xt.arrow/type :union null <Field "my-int" #xt.arrow/type :i64 null>>

  ;; We (XTDB) have two use cases when working with Arrow data. Given a fixed schema we want to write to
  ;; this fixed schema (for example when serializing tx operations we know there is a given set `:put`, `:delete`...).
  ;; The second use case is dynamic writing and copying of user specified data. The main difficulty is
  ;; to have an interface that works for both dynamic schema creation as well as honours a fixed predefined schema.
  ;; The terms reader, writer and copier are not Arrow terms but our own terminology.

  ;; When creating any vector or relation in Arrow you always need an allocator. The allocator keeps
  ;; track of the memory and also assures that you hand it back by closing the resources. The pattern used
  ;; for this is called RAII (https://en.wikipedia.org/wiki/Resource_acquisition_is_initialization).
  ;; So whenever you hand vectors around you need to think about if are you transferring
  ;; ownership or just borrowing the resources.

  ;; From a Field you can simply create a vector

  (with-open [allocator (RootAllocator.)
              my-int-vec (Vector/open allocator #xt/field {"my-int" :i64})]
    (.writeObject my-int-vec 42))

  ;; Let's also now see how you can write to nested types. In the following we creating a struct vector where
  ;; the struct has a predefined field with name `foo` of type `i64`. We are going to see how the
  ;; behaviour for predefined field types differs from dynamic creation.

  ;; When asking for a writer for field `foo` you are going to get a writer with the specified type back.
  (with-open [allocator (RootAllocator.)
              struct-vec (Vector/open allocator #xt/field {"my-struct" [:struct {"foo" :i64}]})]
    (t/is (= #xt/field {"foo" :i64}
             (-> struct-vec
                 (.vectorFor "foo")
                 (.getField)))))

  ;; You can also ask for a writer specifying an arrow type. In this case it works as the writer matches the
  ;; previously specified field type.
  (with-open [allocator (RootAllocator.)
              struct-vec (Vector/open allocator #xt/field {"my-struct" [:struct {"foo" :i64}]})]
    (t/is (= #xt/field {"foo" :i64}
             (-> struct-vec
                 (.vectorFor "foo" #xt.arrow/type :i64 false)
                 (.getField)))))

  ;; You can also create a struct key by providing an arrow type
  (with-open [allocator (RootAllocator.)
              struct-vec (Vector/open allocator #xt/field {"my-struct" :struct})]
    (t/is (= #xt/field {"bar" :utf8}
             (-> struct-vec
                 (.vectorFor "bar" #xt.arrow/type :utf8 false)
                 (.getField)))))

  ;; Lets also look at how one would write to a duv.

  (with-open [allocator (RootAllocator.)
              duv (DenseUnionVector. allocator "my-duv")]
    (.writeObject duv 42)
    (.writeObject duv "forty-two")

    ;; here we first transform from writer to reader then read the objects out
    ;; of the reader
    (t/is (= [42 "forty-two"]
             (.getAsList duv))))

  ;; Writing to a relation is done in the same way as `VectorReader/vectorFor`

  (with-open [allocator (RootAllocator.)
              rel (Relation. allocator)]
    (-> rel
        (.vectorFor "my-i64" #xt.arrow/type :i64 false)
        (.writeLong 42))

    (-> rel
        (.vectorFor "my-union" #xt.arrow/type :union false)
        (.writeObject 42))

    (.endRow rel)

    (-> rel
        (.vectorFor "my-i64" #xt.arrow/type :i64 false)
        (.writeLong 43))

    (-> rel
        (.vectorFor "my-union")
        (.writeObject "forty-three"))

    (.endRow rel)

    (t/is (= [{:my-i64 42, :my-union 42} {:my-i64 43, :my-union "forty-three"}]
             (-> rel
                 (.getAsMaps)))))

  ;; Beware that in order for columns to not be populated the types need to be :unions or nullables as otherwise
  ;; there is no way to signal the absence of that value.

  (with-open [allocator (RootAllocator.)
              rel (Relation. allocator)]
    (.writeObject (.vectorFor rel "my-first-column" #xt.arrow/type :i64 true) 42)
    (.endRow rel)

    (.writeObject (.vectorFor rel "my-second-column" #xt.arrow/type :utf8 true) "forty-three")
    (.endRow rel)

    (t/is (= [{:my-first-column 42} {:my-second-column "forty-three"}]
             (.getAsMaps rel))))
  ;; =>

  ;; Let's now look at the readers and row-copiers. A simple example:

  (with-open [allocator (RootAllocator.)
              my-int-vec (Vector/open allocator #xt/field {"my-int" :i64})]
    ;; writing into the vec
    (.writeLong my-int-vec 42)

    ;; reading from a reader
    (t/is (= 42 (.getLong my-int-vec 0))))

  ;; Readers are mostly used to copy from a relation into new relation writer.
  ;; The following copies the first row of `rel1` into `rel2`.
  '(let [copier (.rowCopier rel2 rel1)]
     (.copyRow copier 0))

  ;; Let us set up a whole example.

  (with-open [allocator (RootAllocator.)
              ;; we will write to the first two writers
              ;; using the standard machinary from above
              rel1 (Relation. allocator)
              rel2 (Relation. allocator)

              ;; we will copy to this writer using a row copier
              rel (Relation. allocator)]

    ;; populating rel1 and rel2
    (let [my-column1 (.vectorFor rel1 "my-column" #xt.arrow/type :union false)
          my-column2 (.vectorFor rel2 "my-column" #xt.arrow/type :union false)]
      (-> (.vectorFor my-column1 "i64" #xt.arrow/type :i64 false)
          (.writeLong 42))
      (-> (.vectorFor my-column2 "utf8" #xt.arrow/type :utf8 false)
          (.writeObject "forty-two")))

    ;; copying to rel3
    (let [copier1 (.rowCopier rel1 rel)
          copier2 (.rowCopier rel2 rel)]
      (.copyRow copier1 0)
      (.copyRow copier2 0))

    ;; checkout of the field type of that column
    (t/is (= #xt/field {"my-column" [:union :i64 :utf8]}
             (.getField (.vectorFor rel "my-column"))))

    (t/is (= [{:my-column 42} {:my-column "forty-two"}]
             (.getAsMaps rel)))))

  ;; What is nice about the above example is that even though the types of "my-column" of rel1 and rel2
  ;; are base types the row copiers take care to create "union" types.

