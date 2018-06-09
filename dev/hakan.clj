(ns hakan
  (:require [clojure.spec.alpha :as s]))

;;; Experiment implementing a parser for a subset of Prolog using spec.

(defn- prolog-var? [s]
  (and (symbol? s)
       (Character/isUpperCase (char (first (name s))))))

(def ^:private separator? '#{:- <- . - ?})

(s/def ::dot #{'.})
(s/def ::var prolog-var?)
(s/def ::literal (complement prolog-var?))
(s/def ::atom (s/or :var ::var :literal ::literal))
(s/def ::predicate (s/cat :name (every-pred symbol? (complement separator?))
                          :args (s/? (s/coll-of ::atom :kind list?))))
(s/def ::fact (s/& (s/cat :name symbol?
                          :args (s/? (s/coll-of ::literal :kind list?))
                          :op (s/or :assert ::dot
                                    :retact #{'-}))
                   (s/conformer #(update % :op first))))
(s/def ::rule (s/& (s/cat :head ::predicate
                          :comma-hyphen #{:- '<-}
                          :body (s/* (s/alt :predicate ::predicate
                                            :expression list?))
                          :dot ::dot)
                   (s/conformer #(dissoc % :comma-hyphen :dot))))
(s/def ::query (s/& (s/cat :query ::predicate
                           :question-mark #{'?})
                    (s/conformer #(dissoc % :question-mark))))
(s/def ::comment (s/& (s/cat :percent #{'%}
                             :comment (s/* (every-pred any? (complement separator?)))
                             :dot ::dot)
                      (s/conformer #(dissoc % :percent :dot))))
(s/def ::clause (s/alt :fact ::fact
                       :rule ::rule
                       :query ::query
                       :comment ::comment))
(s/def ::program (s/* ::clause))

(comment
  (s/conform
   ::program
   '[mother_child(trude, sally).

     father_child(tom, sally).
     father_child(tom, erica).
     father_child(mike, tom).

     sibling(X, Y)      :- parent_child(Z, X), parent_child(Z, Y).

     parent_child(X, Y) :- father_child(X, Y).
     parent_child(X, Y) :- mother_child(X, Y).])

  ;; https://github.com/racket/datalog/tree/master/tests/examples
  (s/conform
   ::program
   '[parent(john,douglas).
     parent(john,douglas)?
     % parent(john, douglas).

     parent(john,ebbon)?

     parent(bob,john).
     parent(ebbon,bob).
     parent(A,B)?
     % parent(john, douglas).
     % parent(bob, john).
     % parent(ebbon, bob).

     parent(john,B)?
     % parent(john, douglas).

     parent(A,A)?

     ancestor(A,B) :- parent(A,B).
     ancestor(A,B) :- parent(A,C), ancestor(C, B).
     ancestor(A, B)?
     % ancestor(ebbon, bob).
     % ancestor(bob, john).
     % ancestor(john, douglas).
     % ancestor(bob, douglas).
     % ancestor(ebbon, john).
     % ancestor(ebbon, douglas).

     ancestor(X,john)?
     % ancestor(bob, john).
     % ancestor(ebbon, john).

     parent(bob, john)-
     parent(A,B)?
     % parent(john, douglas).
     % parent(ebbon, bob).

     ancestor(A,B)?
     % ancestor(john, douglas).
     % ancestor(ebbon, bob).]))
