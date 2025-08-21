(ns xtdb.trie-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]))

(t/use-fixtures :each tu/with-allocator)

(deftest parses-trie-paths
  (letfn [(parse [trie-key]
            (-> (trie/parse-trie-key trie-key)
                (update :part #(some-> % vec))
                (mapv [:level :recency :part :block-idx])))]
    (t/is (= "l00-rc-b04" (trie/->l0-trie-key 4)))
    (t/is (= [0 nil [] 4] (parse "l00-rc-b04")))

    (t/is (= "l06-rc-p0013-b178" (trie/->trie-key 6 nil (byte-array [0 0 1 3]) 120)))
    (t/is (= [6 nil [0 0 1 3] 120] (parse "l06-rc-p0013-b178")))

    (t/is (= "l02-r20240101-b0c"
             (trie/->trie-key 2 #xt/date "2024-01-01" nil 12)
             (trie/->trie-key 2 #xt/date "2024-01-01" (byte-array []) 12)))
    (t/is (= [2 #xt/date "2024-01-01" [] 12] (parse "l02-r20240101-b0c")))

    (t/is (neg? (compare (trie/->trie-key 1 #xt/date "2024-01-01" nil 12)
                         (trie/->trie-key 1 nil nil 12)))
          "current comes lexically after historical")))
