(ns crux.datalog
  "Experiment implementing a parser for a subset of Prolog using spec."
  (:require [clojure.spec.alpha :as s]))

(defn- prolog-var? [s]
  (and (symbol? s)
       (Character/isUpperCase (char (first (name s))))))

(s/def ::dot #{'.})
(s/def ::var prolog-var?)
(s/def ::literal (complement prolog-var?))
(s/def ::atom (s/or :var ::var :literal ::literal))
(s/def ::predicate (s/cat :name symbol?
                          :args (s/? (s/coll-of ::atom :kind list?))))
(s/def ::fact (s/cat :name symbol?
                     :args (s/? (s/coll-of ::literal :kind list?))
                     :dot ::dot))
(s/def ::rule (s/cat :head ::predicate
                     :comma-hyphen #{:-}
                     :body (s/+ ::predicate)
                     :dot ::dot))
(s/def ::clause (s/alt :fact ::fact
                       :rule ::rule))
(s/def ::program (s/* ::clause))
(s/def ::query (s/cat :query ::predicate
                      :dot ::dot))

(comment
  (s/conform
   ::program
   '[mother_child(trude, sally).

     father_child(tom, sally).
     father_child(tom, erica).
     father_child(mike, tom).

     sibling(X, Y)      :- parent_child(Z, X), parent_child(Z, Y).

     parent_child(X, Y) :- father_child(X, Y).
     parent_child(X, Y) :- mother_child(X, Y).]))
