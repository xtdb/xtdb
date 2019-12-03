(ns webservice.util)

(defn uuid [] (java.util.UUID/randomUUID))

(defn map-vals [f m] (reduce-kv (fn [m k v] (assoc m k (f v))) {} m))

(defn find-key [v m] (filter (comp #{v} m) (keys m)))

(defn deep-merge [a & maps]
  (if (map? a)
    (apply merge-with deep-merge a maps)
    (apply merge-with deep-merge maps)))

(defn contains-kv?
  "Returns true if the key k is present in the given map m and it's value matches v."
  [k v m]
  (= (m k) v))
