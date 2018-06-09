(ns hakan
  (:require [clojure.spec.alpha :as s]))

;;; Experiment implementing a parser for a subset of Prolog using spec.

;; See Racket for the Datalog syntax in EBNF.
;; https://docs.racket-lang.org/datalog/datalog.html

(defn- prolog-var? [s]
  (and (symbol? s)
       (Character/isUpperCase (char (first (name s))))))

(s/def ::program (s/* ::statement))
(s/def ::statement (s/alt :assertion ::assertion
                          :retraction ::retraction
                          :query ::query))
(s/def ::assertion (s/cat :clause ::clause
                          :dot #{'.}))
(s/def ::retraction (s/cat :clause ::clause
                           :tilde #{'-}))
(s/def ::query (s/cat :literal ::literal
                      :question-mark #{'?}))
(s/def ::clause (s/alt :rule (s/cat :literal ::literal
                                    :comma-hypen #{:-}
                                    :body ::body)
                       :fact ::literal))
(s/def ::body (s/+ ::literal))
(s/def ::literal (s/alt :predicate (s/cat :symbol ::identifier
                                          :terms (s/? (s/coll-of ::term :kind list?)))
                        :equality-predicate ::equality-predicate))
(s/def ::equality-predicate (s/and list? (s/cat :op '#{= !=}
                                                :terms (s/+ ::term))))
(s/def ::term (s/or :variable ::variable
                    :constant ::constant))

(s/def ::constant (complement (some-fn list? prolog-var?)))
(s/def ::identifier (s/and symbol? (complement (some-fn prolog-var? '#{. - ? = %}))))
(s/def ::variable prolog-var?)

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
     ;; % parent(john, douglas).

     parent(john,ebbon)?

     parent(bob,john).
     parent(ebbon,bob).
     parent(A,B)?
     ;; % parent(john, douglas).
     ;; % parent(bob, john).
     ;; % parent(ebbon, bob).

     parent(john,B)?
     ;; % parent(john, douglas).

     parent(A,A)?

     ancestor(A,B) :- parent(A,B).
     ancestor(A,B) :- parent(A,C), ancestor(C, B).
     ancestor(A, B)?
     ;; % ancestor(ebbon, bob).
     ;; % ancestor(bob, john).
     ;; % ancestor(john, douglas).
     ;; % ancestor(bob, douglas).
     ;; % ancestor(ebbon, john).
     ;; % ancestor(ebbon, douglas).

     ancestor(X,john)?
     ;; % ancestor(bob, john).
     ;; % ancestor(ebbon, john).

     parent(bob, john)-
     parent(A,B)?
     ;; % parent(john, douglas).
     ;; % parent(ebbon, bob).

     ancestor(A,B)?
     ;; % ancestor(john, douglas).
     ;; % ancestor(ebbon, bob).
     ]))

;;; https://docs.racket-lang.org/datalog/Parenthetical_Datalog_Module_Language.html
;; (! (parent john douglas))
;; (? (parent john douglas))

;; (? (parent john ebbon))

;; (! (parent bob john))
;; (! (parent ebbon bob))
;; (? (parent A B))

;; (? (parent john B))

;; (? (parent A A))

;; (! (:- (ancestor A B)
;;        (parent A B)))
;; (! (:- (ancestor A B)
;;        (parent A C)
;;        (ancestor C B)))
;; (? (ancestor A B))

;; (? (ancestor X john))

;; (~ (parent bob john))

;; (? (parent A B))

;; (? (ancestor A B))
