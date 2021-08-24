(ns crux.codec-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.memory :as mem]
            [crux.fixtures :as fix]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.api :as crux]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [clojure.spec.alpha :as s])
  (:import crux.codec.Id
           java.math.BigDecimal
           org.agrona.MutableDirectBuffer
           [java.time Instant ZonedDateTime LocalDate ZoneId Duration]
           [java.util Arrays Date]
           java.net.URL))

(t/use-fixtures :each fix/with-silent-test-check)

(def ^:private gen-date
  (->> gen/large-integer (gen/fmap #(Date. ^long %))))

(def ^:private gen-instant
  (->> (gen/tuple (gen/large-integer* {:min (.getEpochSecond Instant/MIN)
                                       :max (.getEpochSecond Instant/MAX)})
                  (gen/large-integer* {:min (- (dec 1e9))
                                       :max (dec 1e9)}))
       (gen/fmap (fn [[s ns]]
                   (Instant/ofEpochSecond s ns)))))

(def ^:private gen-zdt
  (let [utc (ZoneId/of "UTC")]
    (->> gen-instant
         (gen/fmap #(ZonedDateTime/ofInstant % utc)))))

(def ^:private primitive-generators
  [(gen/return nil)
   gen/large-integer
   (->> gen/double (gen/such-that #(Double/isFinite %)))
   gen-date
   gen/string
   gen/char
   gen/boolean
   (-> gen/keyword (vary-meta assoc ::sortable? false))
   (-> gen/uuid (vary-meta assoc ::sortable? false))
   (-> gen/bytes (vary-meta assoc ::sortable? false))
   (->> gen/double
        (gen/such-that #(Double/isFinite %))
        (gen/fmap #(BigDecimal/valueOf ^double %)))
   (->> gen/large-integer (gen/fmap biginteger))
   (->> gen/large-integer (gen/fmap bigint))
   (->> gen-zdt (gen/fmap #(.toLocalDate ^ZonedDateTime %)))
   (->> gen-zdt (gen/fmap #(.toLocalTime ^ZonedDateTime %)))
   (->> gen-zdt (gen/fmap #(.toLocalDateTime ^ZonedDateTime %)))
   gen-instant
   (->> (gen/tuple gen/large-integer gen/small-integer)
        (gen/fmap (fn [[s ns]]
                    (Duration/ofSeconds s ns))))])

(t/deftest test-double-nan
  (let [encoded-nan (c/->value-buffer ##NaN)]
    (t/is (c/can-decode-value-buffer? encoded-nan))
    (t/is (Double/isNaN (c/decode-value-buffer encoded-nan)))))

(tcct/defspec test-generative-primitive-value-decoder 1000
  (prop/for-all [v (gen/one-of primitive-generators)]
                (let [buffer (c/->value-buffer v)]
                  (if (c/can-decode-value-buffer? buffer)
                    (if (bytes? v)
                      (Arrays/equals ^bytes v ^bytes (c/decode-value-buffer buffer))
                      (= v (c/decode-value-buffer buffer)))

                    (cond
                      (and (string? v)
                           (< @#'c/max-value-index-length (alength (.getBytes ^String v "UTF-8"))))
                      (= @#'c/clob-value-type-id
                         (.getByte (c/value-buffer-type-id buffer) 0))

                      (and (bytes? v)
                           (< @#'c/max-value-index-length (alength ^bytes (mem/->on-heap (mem/->nippy-buffer v)))))
                      (= @#'c/object-value-type-id
                         (.getByte (c/value-buffer-type-id buffer) 0))

                      :else false)))))

(def ^:private byte-array-class
  (Class/forName "[B"))

(defn- no-negative-zeros [vs]
  ;; HACK filter out -0.0 because Java doesn't sort -0.0 ahead of 0.0, but Crux does
  (remove (every-pred double? zero? #(not= 0 (Double/doubleToLongBits %))) vs))

(tcct/defspec test-ordering-of-values 100
  (prop/for-all [values (gen/one-of (->> primitive-generators
                                         (remove (comp false? ::sortable? meta))
                                         (map (fn [gen]
                                                (->> (gen/vector gen 10)
                                                     (gen/fmap no-negative-zeros))))))]
                (let [value+buffer (for [v values]
                                     [v (c/->value-buffer v)])]

                  (t/is (= (sort-by first value+buffer)
                           (sort-by second mem/buffer-comparator value+buffer))))))

(t/deftest test-string-prefix
  (t/testing "string encoding size overhead"
    (t/is (= (+ c/value-type-id-size
                (alength (.getBytes "Hello" "UTF-8"))
                @#'c/string-terminate-mark-size)
             (mem/capacity (c/->value-buffer "Hello")))))

  (t/testing "a short encoded string is not a prefix of a longer string"
    (let [hello (c/->value-buffer "Hello")
          hello-world (c/->value-buffer "Hello World")]
      (t/is (false? (mem/buffers=? hello hello-world (mem/capacity hello))))))

  (t/testing "a short raw string is a prefix of a longer string"
    (let [hello (mem/as-buffer (.getBytes "Hello" "UTF-8"))
          hello-world (mem/as-buffer (.getBytes "Hello World" "UTF-8"))]
      (t/is (true? (mem/buffers=? hello hello-world (mem/capacity hello))))))

  (t/testing "cannot decode non-terminated string"
    (let [hello (c/->value-buffer "Hello")
          hello-prefix (mem/slice-buffer hello 0 (- (mem/capacity hello) @#'c/string-terminate-mark-size))]
      (t/is (thrown-with-msg? AssertionError #"String not terminated." (c/decode-value-buffer hello-prefix))))))

(t/deftest test-id-reader
  (t/testing "can read and convert to real id"
    (t/is (not= (c/new-id "http://google.com") #xt/id "http://google.com"))
    (t/is (= "234988566c9a0a9cf952cec82b143bf9c207ac16"
             (str #xt/id "http://google.com")))
    (t/is (instance? Id (c/new-id #xt/id "http://google.com"))))

  (t/testing "can create different types of ids"
    (t/is (= (c/new-id :foo) #xt/id ":foo"))
    (t/is (= (c/new-id #uuid "37c20bcd-eb5e-4ef7-b5dc-69fed7d87f28")
             #xt/id "37c20bcd-eb5e-4ef7-b5dc-69fed7d87f28"))
    (t/is (not= #xt/id "234988566c9a0a9cf952cec82b143bf9c207ac16"
                (c/new-id "234988566c9a0a9cf952cec82b143bf9c207ac16")))
    (t/is (not= (c/new-id "234988566c9a0a9cf952cec82b143bf9c207ac16")
                #xt/id "234988566c9a0a9cf952cec82b143bf9c207ac16")))

  (t/testing "legacy #crux/* reader tags"
    (t/is (= (c/new-id :foo) #crux/id ":foo"))
    (t/is (= (c/new-id #uuid "37c20bcd-eb5e-4ef7-b5dc-69fed7d87f28")
             #crux/id "37c20bcd-eb5e-4ef7-b5dc-69fed7d87f28"))
    (t/is (not= #crux/id "234988566c9a0a9cf952cec82b143bf9c207ac16"
                (c/new-id "234988566c9a0a9cf952cec82b143bf9c207ac16")))
    (t/is (not= (c/new-id "234988566c9a0a9cf952cec82b143bf9c207ac16")
                #crux/id "234988566c9a0a9cf952cec82b143bf9c207ac16")))

  (t/testing "can embed id in other forms"
    (t/is (not= {:find ['e]
                 :where [['e (c/new-id "http://xmlns.com/foaf/0.1/firstName") "Pablo"]]}
                '{:find [e]
                  :where [[e #xt/id "http://xmlns.com/foaf/0.1/firstName" "Pablo"]]})))

  (t/testing "URL and keyword are same id"
    (t/is (= (c/new-id (keyword "http://xmlns.com/foaf/0.1/firstName"))
             #xt/id "http://xmlns.com/foaf/0.1/firstName"))
    (t/is (= (c/new-id (URL. "http://xmlns.com/foaf/0.1/firstName"))
             #xt/id ":http://xmlns.com/foaf/0.1/firstName"))
    (t/is (not= (c/new-id "http://xmlns.com/foaf/0.1/firstName")
                #xt/id ":http://xmlns.com/foaf/0.1/firstName"))))

(t/deftest test-base64-reader
  (t/is (Arrays/equals (byte-array [1 2 3])
                       ^bytes (c/read-edn-string-with-readers "#xt/base64 \"AQID\"")))

  (t/is (Arrays/equals (byte-array [1 2 3])
                       ^bytes (c/read-edn-string-with-readers "#crux/base64 \"AQID\""))
        "legacy reader tag"))

(t/deftest test-unordered-coll-hashing-1001
  (let [foo-a {{:foo 1} :foo1
               {:foo 2} :foo2}
        foo-b {{:foo 2} :foo2
               {:foo 1} :foo1}]
    (t/is (not= (seq foo-a) (seq foo-b))) ; ordering is different
    (t/is (thrown? ClassCastException (sort foo-a))) ; can't just sort it
    (t/is (= #xt/id "952fe1092dacf06dc4d9270ce1d27010a6c46508"
             (c/new-id {:xt/id :foo
                        :foo foo-a})
             (c/new-id {:xt/id :foo
                        :foo foo-b}))))

  (let [foo #{#{:foo} #{:bar}}]
    (t/is (thrown? ClassCastException (sort foo)))
    (t/is (= #xt/id "3d9559394baad1e185c228dfc5a9f1eb655279fd"
             (c/new-id {:xt/id :foo
                        :foo foo}))))

  (let [foo #{42 "hello"}]
    (t/is (thrown? ClassCastException (sort foo)))
    (t/is (= #xt/id "e744f8a121d024c68e20f160b0965b1bacf1bf29"
             (c/new-id {:xt/id :foo
                        :foo foo}))))

  (t/testing "original coll hashing unaffected"
    (t/is (= #xt/id "f5282928a8acc2ac6bfc796fea2a676a9bdadfd5"
             (c/new-id {:xt/id :foo
                        :foo {:a 1, :b 2}})
             (c/new-id {:xt/id :foo
                        :foo {:b 2, :a 1}})))))

(t/deftest test-java-type-serialisation-1044
  (with-open [node (crux/start-node {})]
    (let [doc {:xt/id :foo
               :date (java.util.Date.)
               :uri (java.net.URI. "https://google.com")
               :url (java.net.URL. "https://google.com")
               :uuid (java.util.UUID/randomUUID)}]
      (fix/submit+await-tx node [[:crux.tx/put doc]])
      (t/is (= doc (crux/entity (crux/db node) :foo))))))
