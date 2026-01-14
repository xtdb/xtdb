(ns xtdb.vector.reader-test
  (:require [clojure.test :as t]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [xtdb.test-generators :as tg]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [org.apache.arrow.memory BufferAllocator]
           (xtdb.arrow Vector)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest ^:property vector-read-what-you-write
  (tu/run-property-test
    {:num-tests tu/property-test-iterations}
    (prop/for-all [{:keys [vs] :as vec-gen} tg/vector-vs-gen]
      (with-open [^Vector src-vec (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)]
        (tg/lists-equal-normalized? vs (.getAsList src-vec))))))

(defn- copy-vector ^Vector
  ([^Vector src-vec ^BufferAllocator al]
   (copy-vector src-vec al 0 (.getValueCount src-vec)))
  ([^Vector src-vec ^BufferAllocator al start-idx end-idx]
   (util/with-close-on-catch [out-vec (Vector/open al (.getField src-vec))]
     (let [copier (.rowCopier src-vec out-vec)]
       (doseq [i (range start-idx end-idx)]
         (.copyRow copier i))
       out-vec))))

(defn- vectors-equal?
  [^Vector src-vec ^Vector out-vec]
  (and (= (.getValueCount src-vec) (.getValueCount out-vec))
       (tg/lists-equal-normalized? (.getAsList src-vec) (.getAsList out-vec))))

(t/deftest ^:property full-vector-copy-preserves-data
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [vec-gen tg/vector-vs-gen]
                 (with-open [^Vector src-vec (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)
                             ^Vector copied-vec (copy-vector src-vec tu/*allocator*)]
                   (vectors-equal? src-vec copied-vec)))))

(t/deftest ^:property partial-vector-copy-preserves-data
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [vec-gen (tg/fixed-length-vector-vs-gen 100)
                  start-idx (gen/choose 0 50)
                  end-idx (gen/choose 51 100)]
                 (with-open [^Vector src-vec (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)
                             ^Vector copied-vec (copy-vector src-vec tu/*allocator* start-idx end-idx)]
                   (let [expected-data (subvec (:vs vec-gen) start-idx end-idx)
                         actual-data (.getAsList copied-vec)]
                     (tg/lists-equal-normalized? expected-data actual-data))))))

(defn- merge-vectors-into-duv ^Vector [^BufferAllocator al vectors]
  (util/with-close-on-catch [^Vector duv-vec (Vector/open al #xt/field {"mixed" :union})]
    (doseq [^Vector vec vectors]
      (let [copier (.rowCopier vec duv-vec)]
        (dotimes [i (.getValueCount vec)]
          (.copyRow copier i))))

    duv-vec))

(t/deftest ^:property copy-two-distinct-single-typed-vectors
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [[vec-gen1 vec-gen2] tg/two-distinct-single-type-vecs-gen]
                 (with-open [^Vector src-vec1 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen1)
                             ^Vector src-vec2 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen2)
                             ^Vector duv (merge-vectors-into-duv tu/*allocator* [src-vec1 src-vec2])]
                   (let [expected-data (concat (:vs vec-gen1) (:vs vec-gen2))
                         actual-data (.getAsList duv)]
                     (tg/lists-equal-normalized? expected-data actual-data))))))

(t/deftest ^:property copy-two-duvs
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [[vec-gen1 vec-gen2] tg/two-distinct-duvs-gen]
                 (with-open [^Vector src-vec1 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen1)
                             ^Vector src-vec2 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen2)
                             ^Vector duv (merge-vectors-into-duv tu/*allocator* [src-vec1 src-vec2])]
                   (let [expected-data (concat (:vs vec-gen1) (:vs vec-gen2))
                         actual-data (.getAsList duv)]
                     (tg/lists-equal-normalized? expected-data actual-data))))))

(t/deftest ^:property multiple-type-promotions
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [vec-gen tg/vector-vs-gen
                  arrow-types (gen/vector tg/arrow-type-gen 1 4)]
                 (with-open [^Vector promoted-vec (reduce
                                                   (fn [^Vector old-vec arrow-type]
                                                     (util/with-close-on-catch [old-vec old-vec]
                                                       (.maybePromote$xtdb_core old-vec tu/*allocator*
                                                                                arrow-type
                                                                                (= #xt.arrow/type :null arrow-type))))
                                                   (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)
                                                   arrow-types)]
                   promoted-vec))))

(t/deftest ^:property multiple-open-slice-calls
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [vec-gen tg/vector-vs-gen
                  open-slice-count (gen/choose 1 10)]
                 (with-open [^Vector src-vec1 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)]
                   (let [slices (repeatedly open-slice-count #(.openSlice src-vec1 tu/*allocator*))] 
                     (and (every? #(vectors-equal? src-vec1 %) slices)
                          (every? #(do (.close ^Vector %) true) slices)))))))
