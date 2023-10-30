(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false} arrow-readers-writers-doc
  (:require [xtdb.vector.writer :as vw]
            [xtdb.vector.reader :as vr]
            [xtdb.types :as types])
  (:import (org.apache.arrow.vector.complex DenseUnionVector)
           (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector.types.pojo FieldType Field)))

;; The namespace is documentation illustrating how readers, writers and copiers work as well as a
;; give a pragmatic intro to Arrow.

;; Before reading this one should be somehow familiar with the concepts of
;; https://arrow.apache.org/docs/format/Columnar.html#format-columnar
;; Get familiar with the terminology section and read the start of the physical memory layout.
;; You don't need to know every physical memory layout, but maybe look at one or two to get a feel
;; for things.

;; Our engine works on relations. Think of a relation as a set of vectors where each vector has a name.
;; Lets say you submit the following two documents.
[:put :xt-docs {:xt/id 1 :column-name 42}]
[:put :xt-docs {:xt/id 2 :column-name "foo"}]
;; then this gets transformed into the relation (forgetting the special treatement of `xt/id` for the moment)
{"xt/id" [1 2]
 "column-name" [42 "foo"]}
;; where the keys are the names and the value vectors are actual Arrow Vectors.

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

;; FieldType adds nullability to ArrowTypes
(FieldType/notNullable #xt.arrow/type :i64)
;; => <FieldType #xt.arrow/type :i64 not-null>

(FieldType/nullable #xt.arrow/type :union)
;; => <FieldType #xt.arrow/type :union null>

;; Fields are essentially FieldType + naming + potential child Fields
(Field. "my-int" (FieldType/nullable #xt.arrow/type :i64) nil)
;; => <Field "my-int" #xt.arrow/type :i64 null>

;; See how we are specifying a child here.
(Field. "my-union" (FieldType/nullable #xt.arrow/type :union)
        [(Field. "my-int" (FieldType/nullable #xt.arrow/type :i64) nil)])
;; => <Field "my-union" #xt.arrow/type :union null <Field "my-int" #xt.arrow/type :i64 null>>

;; We (XTDB) have two use cases when working with Arrow data. Given a fixed schema we want to write to
;; this fixed schema (for example when serializing tx operations we know there is a given set `:put`, `:delete`...).
;; The second use case is dynamic writing and copying of user specified data. The main difficulty is
;; to have an interface that works for both dynamic schema creation as well as honours a fixed predefined schema.
;; The terms reader, writer and copier are not Arrow terms but our own terminology. As there name suggests,
;; they are objects (often reify's) on top of Arrow objects to help us read and write to
;; relations as well as copy data between relations.

;; When creating any vector or relation in Arrow you always need an allocator. The allocator keeps
;; track of the memory and also assures that you hand it back by closing the resources. The pattern used
;; for this is called RAII (https://en.wikipedia.org/wiki/Resource_acquisition_is_initialization).
;; So whenever you hand Arrow vectors around you need to think about if are you transferring
;; ownership or just borrowing the resources.

;; From a Field you can simply create a vector

(with-open [allocator (RootAllocator.)
            my-int-vec (.createVector (types/col-type->field "my-int" :i64) allocator)])

;; If you want to write to a vector you can create a writer.

(with-open [allocator (RootAllocator.)
            my-int-vec (.createVector (types/col-type->field "my-int" :i64) allocator)]
  (let [my-int-wrt (vw/->writer my-int-vec)]
    (vw/write-value! 42 my-int-wrt)))

;; Beware that if you don't close the vector the allocator will complain.
(with-open [allocator (RootAllocator.)]
  (let [my-int-vec (.createVector (types/col-type->field "my-int" :i64) allocator)
        my-int-wrt (vw/->writer my-int-vec)]
    (vw/write-value! 42 my-int-wrt)))

;; Let's also now see how you can write to nested types. In the following we creating a struct vector where
;; the struct has a predefined field with name `foo` of type `i64`. We are going to see how the
;; behaviour for predefined field types differs from dynamic creation.

;; When asking for a writer for field `foo` you are going to get a writer with the specified type back.
(with-open [allocator (RootAllocator.)
            struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {foo :i64}]) allocator)]
  (-> (vw/->writer struct-vec)
      (.structKeyWriter "foo")
      (.getField)))
;; => <Field "foo" #xt.arrow/type :i64 not-null>

;; You can also ask for a writer specifying a field type. In this case it works as the writer matches the
;; previously specified field type.
(with-open [allocator (RootAllocator.)
            struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {foo :i64}]) allocator)]
  (-> (vw/->writer struct-vec)
      (.structKeyWriter "foo" (FieldType/notNullable #xt.arrow/type :i64))
      (.getField)))
;; => <Field "foo" #xt.arrow/type :i64 not-null>

;; If you are asking for a different field type then the one specified in the beginning you are going to get
;; an type mismatch error.
(with-open [allocator (RootAllocator.)
            struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {foo :i64}]) allocator)]
  (-> (vw/->writer struct-vec)
      (.structKeyWriter "foo" (FieldType/notNullable #xt.arrow/type :utf8))
      (.getField)))

;; Lets now look at the defaulting behaviour if you do not specify the field type up front.
;; By default you are going to get a `:union` type. The idea being that we don't know yet all the different
;; types that are going to be written to this field.
(with-open [allocator (RootAllocator.)
            struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {}]) allocator)]
  (-> (vw/->writer struct-vec)
      (.structKeyWriter "bar")
      (.getField)))
;; => <Field "bar" #xt.arrow/type :union not-null>

;; But you can also ask for a type explicitly and then it will be honoured.
(with-open [allocator (RootAllocator.)
            struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {}]) allocator)]
  (-> (vw/->writer struct-vec)
      (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :utf8))
      (.getField)))
;; => <Field "bar" #xt.arrow/type :utf8 not-null>

;; Beware that you can not first ask for one type and then later for different one, hoping that the
;; underlying vector gets promoted to a union vector.
(with-open [allocator (RootAllocator.)
            struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {}]) allocator)]
  (-> (vw/->writer struct-vec)
      (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :utf8))
      (.getField))

  (-> (vw/->writer struct-vec)
      (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :i64))
      (.getField)))

;; Lets also look at how one would write to a duv.

(with-open [allocator (RootAllocator.)
            duv (DenseUnionVector/empty "my-duv" allocator)]
  (let [duv-writer (vw/->writer duv)]
    (->> (.legWriter duv-writer :foo (FieldType/notNullable #xt.arrow/type :i64))
         (vw/write-value! 42))
    (->> (.legWriter duv-writer :bar (FieldType/notNullable #xt.arrow/type :utf8))
         (vw/write-value! "forty-two"))
    ;; here we first transform from writer to reader then read the objects out
    ;; of the reader
    (let [rdr (vw/vec-wtr->rdr duv-writer)]
      (->> (for [i (range (.valueCount rdr))]
             (.getObject rdr i))
           (into [])))))
;; => [42 "forty-two"]

;; Writing to a relation is done similarly to `structKeyWriter` but with the method being called `colWriter`.

(with-open [allocator (RootAllocator.)
            rel-wtr (vw/->rel-writer allocator)]
  (.startRow rel-wtr)
  (-> rel-wtr
      (.colWriter "my-i64" (FieldType/notNullable #xt.arrow/type :i64))
      (.writeLong 42))
  (-> rel-wtr
      (.colWriter "my-union")
      (.legWriter :i64 (FieldType/notNullable #xt.arrow/type :i64))
      (.writeLong 42))
  (.endRow rel-wtr)
  (.startRow rel-wtr)
  (-> rel-wtr
      (.colWriter "my-i64" (FieldType/notNullable #xt.arrow/type :i64))
      (.writeLong 43))
  (-> rel-wtr
      (.colWriter "my-union")
      (.legWriter :utf8 (FieldType/notNullable #xt.arrow/type :utf8))
      (.writeObject "forty-three"))
  (.endRow rel-wtr)
  (-> (vw/rel-wtr->rdr rel-wtr)
      (vr/rel->rows)))
;; => [{:my-i64 42, :my-union 42} {:my-i64 43, :my-union "forty-three"}]

;; Beware that in order for columns to not be populated the types need to be :unions or nullables as otherwise
;; there is no way to signal the absence of that value.

(with-open [allocator (RootAllocator.)
            rel-wtr (vw/->rel-writer allocator)]
  (.startRow rel-wtr)
  (-> rel-wtr
      (.colWriter "my-first-column")
      (.legWriter :i64 (FieldType/notNullable #xt.arrow/type :i64))
      (.writeLong 42))
  (.endRow rel-wtr)
  (.startRow rel-wtr)
  (-> rel-wtr
      (.colWriter "my-second-column")
      (.legWriter :utf8 (FieldType/notNullable #xt.arrow/type :utf8))
      (.writeObject "forty-three"))
  (.endRow rel-wtr)
  (-> (vw/rel-wtr->rdr rel-wtr)
      (vr/rel->rows)))
;; => [{:my-first-column 42} {:my-second-column "forty-three"}]

;; Let's now look at the readers and row-copiers. Readers are quite similar in nature (walking nested structures)
;; to writers except that one reads from them. A simple example:

(with-open [allocator (RootAllocator.)
            my-int-vec (.createVector (types/col-type->field "my-int" :i64) allocator)]
  ;; writing into the vec
  (let [my-int-wrt (vw/->writer my-int-vec)]
    (vw/write-value! 42 my-int-wrt)

    ;; reading from a reader
    (-> (vw/vec-wtr->rdr my-int-wrt)
        (.getLong 0))))

;; Readers are mostly used to copy from a relation into new relation writer.
;; The following copies the first row of `rel-wtr1` into `rel-wtr2`.
(let [copier (.rowCopier rel-wtr2 (vw/rel-wtr->rdr rel-wtr1))]
  (.copyRow copier 0))

;; Let us set up a whole example.

(with-open [allocator (RootAllocator.)
            ;; we will write to the first two writers
            ;; using the standard machinary from above
            rel-wtr1 (vw/->rel-writer allocator)
            rel-wtr2 (vw/->rel-writer allocator)

            ;; we will copy to this writer using a row copier
            rel-wtr3 (vw/->rel-writer allocator)]

  ;; populating rel-wtr1 and rel-wtr2
  (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :i64))
        my-colun-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :utf8))]
    (.writeLong my-column-wtr1 42)
    (.writeObject my-colun-wtr2 "forty-two"))

  ;; copying to rel-wtr3
  (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
        copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
    (.copyRow copier1 0)
    (.copyRow copier2 0))

  ;; checkout of the field type of that column
  (prn (.getField (.colWriter rel-wtr3 "my-column")))

  (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3)))
;; => [{:my-column 42} {:my-column "forty-two"}]

;; What is nice about the above example is that even though the types of "my-column" of rel-wtr1 and rel-wtr2
;; are base types the row copiers take care to create "union" types.
