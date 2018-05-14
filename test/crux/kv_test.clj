(ns crux.kv-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f :refer [*kv*]]
            [crux.byte-utils :as bu]
            [crux.kv :as cr])
  (:import [java.net URI]))

(t/use-fixtures :each f/start-system)

(def test-eid 1)

(t/deftest test-can-get-at-now
  (cr/-put *kv* [[test-eid :foo "Bar4"]])
  (t/is (= "Bar4" (cr/-get-at *kv* test-eid :foo)))
  (cr/-put *kv* [[test-eid :foo "Bar5"]])
  (t/is (= "Bar5" (cr/-get-at *kv* test-eid :foo)))

  ;; Insert into past
  (cr/-put *kv* [[test-eid :foo "foo1"]] #inst "2000-02-02")
  (t/is (= "Bar5" (cr/-get-at *kv* test-eid :foo))))

(t/deftest test-can-get-at-now-for-old-entry
  (cr/-put *kv* [[test-eid :foo "Bar3"]] #inst "2010-02-02")
  (t/is (= "Bar3" (cr/-get-at *kv* test-eid :foo))))

(t/deftest test-can-get-at-t
  (cr/-put *kv* [[test-eid :foo "Bar3"]] #inst "1901-01-31")
  (t/is (= "Bar3" (cr/-get-at *kv* test-eid :foo #inst "1901-02-01")))

  (cr/-put *kv* [[test-eid :foo "Bar4"]] #inst "1901-02-02")
  (cr/-put *kv* [[test-eid :foo "Bar5"]] #inst "1901-02-03")
  (cr/-put *kv* [[test-eid :foo "Bar6"]] #inst "1901-02-04")

  (t/is (= "Bar3" (cr/-get-at *kv* test-eid :foo #inst "1901-02-01")))
  (t/is (= "Bar4" (cr/-get-at *kv* test-eid :foo #inst "1901-02-02")))
  (t/is (= "Bar6" (cr/-get-at *kv* test-eid :foo #inst "1901-02-05"))))

(t/deftest test-can-get-nil-before-range
  (cr/-put *kv* [[test-eid :foo "Bar3"]] #inst "1901-02-02")
  (cr/-put *kv* [[test-eid :foo "Bar4"]] #inst "1901-02-03")
  (t/is (not (cr/-get-at *kv* test-eid :foo #inst "1901-01-31"))))

(t/deftest test-can-get-nil-outside-of-range
  (cr/-put *kv* [[test-eid :foo "Bar3"]] #inst "1986-10-22")
  (cr/-put *kv* [[test-eid :tar "Bar4"]] #inst "1986-10-22")
  (t/is (not (cr/-get-at *kv* test-eid :tar #inst "1986-10-21"))))

(t/deftest test-entity-ids
  (let [eid (cr/next-entity-id *kv*)]
    (dotimes [n 1000]
      (cr/next-entity-id *kv*))

    (t/is (= (+ eid 1001) (cr/next-entity-id *kv*)))))

(t/deftest test-write-and-fetch-entity
  (let [person (first f/people)
        eid (first (vals (cr/-put *kv* [person] #inst "1986-10-22")))]
    (t/is (= (dissoc person :crux.kv/id)
             (dissoc (cr/entity *kv* eid) :crux.kv/id)))))

(t/deftest test-fetch-entity-at-t
  (let [person (first f/people)
        eid (first (vals (cr/-put *kv* [(assoc person :name "Fred")] #inst "1986-10-22")))]
    (cr/-put *kv* [(assoc person :name "Freda" :crux.kv/id eid)] #inst "1986-10-24")
    (t/is (= "Fred"
             (:name (cr/entity *kv* eid #inst "1986-10-23"))))
    (t/is (= "Freda"
             (:name (cr/entity *kv* eid))))))

;; TODO - re-establish schema checking (existing of ident and type) as
;; middleware?
#_(t/deftest test-invalid-attribute-exception
  (try
    (cr/-put *kv* [[test-eid :unknown-attribute "foo1"]] #inst "1986-10-22")
    (assert false "Exception expected")
    (catch IllegalArgumentException e
      (t/is (= "Unrecognised schema attribute: :unknown-attribute"
               (.getMessage e))))))

(t/deftest test-transact-schema-attribute
  (cr/-put *kv* [[test-eid :new-ident "foo1"]])
  (t/is (= "foo1" (cr/-get-at *kv* test-eid :new-ident)))

  (cr/-put *kv* [[test-eid :new-ident2 1]])
  (t/is (= 1 (cr/-get-at *kv* test-eid :new-ident2)))

  (cr/-put *kv* [[2 :new-ident2 "stringversion"]])
  (t/is (= "stringversion" (cr/-get-at *kv* 2 :new-ident2))))

(t/deftest test-retract-attribute
  (cr/-put *kv* [[test-eid :foo "foo1"]] #inst "1986-10-22")
  (cr/-put *kv* [[test-eid :foo nil]])
  (t/is (not (cr/-get-at *kv* test-eid :foo)))
  (t/is (= "foo1" (cr/-get-at *kv* test-eid :foo #inst "1986-10-22"))))

(t/deftest test-get-attributes
  (cr/-put *kv* [[test-eid :foo/new-ident2 :a]])
  (t/is (= #{:foo/new-ident2}
           (set (keys @(:attributes *kv*))))))

(t/deftest test-primitives
  (cr/-put *kv* [[test-eid :foo "foo1"]])
  (t/is (= "foo1" (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo 1]])
  (t/is (= 1 (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo (byte 1)]])
  (t/is (= 1 (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo (short 1)]])
  (t/is (= 1 (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo (int 1)]])
  (t/is (= 1 (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo 1.0]])
  (t/is (= 1.0 (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo (float 1.0)]])
  (t/is (= 1.0 (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo 1M]])
  (t/is (= 1M (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo (biginteger 1)]])
  (t/is (= (biginteger 1) (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo #inst "2001-01-01"]])
  (t/is (= #inst "2001-01-01" (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo true]])
  (t/is (= true (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo #uuid "fbee1a5e-273b-4d70-9fde-be76be89209a"]])
  (t/is (=  #uuid "fbee1a5e-273b-4d70-9fde-be76be89209a"
            (cr/-get-at *kv* test-eid :foo)))

  (cr/-put *kv* [[test-eid :foo (byte-array [1 2])]])
  (t/is (bu/bytes=? (byte-array [1 2]) (cr/-get-at *kv* test-eid :foo)))


  (cr/-put *kv* [[test-eid :foo (URI. "http://google.com/")]])
  (t/is (= (URI. "http://google.com/") (cr/-get-at *kv* test-eid :foo)))


  (cr/-put *kv* [[test-eid :foo [1 2]]])
  (t/is (= [1 2] (cr/-get-at *kv* test-eid :foo))))

(t/deftest test-store-and-retrieve-meta
  (t/is (nil? (cr/get-meta *kv* :foo)))
  (cr/store-meta *kv* :foo {:bar 2})
  (t/is (= {:bar 2} (cr/get-meta *kv* :foo))))
