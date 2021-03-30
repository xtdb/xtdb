(ns core2.ghd-test
  (:require [clojure.test :as t]
            [core2.ghd :as ghd]))

(def backtracking-paper-graph '[[:A a b c]
                                [:B d e f]
                                [:C c d g]
                                [:D a f i]
                                [:E g i]
                                [:F b e h]
                                [:G e j]
                                [:H a h j]])

(t/deftest can-separate-components
  (let [h (ghd/->hgraph backtracking-paper-graph)]
    (t/is (= [#{:A :B :C :D :E :F :G :H}]
             (ghd/separate h #{:A :B :C :D :E :F :G :H} #{})))

    (t/is (= [#{:B :C :D :E :F :G :H}]
             (ghd/separate h #{:A :B :C :D :E :F :G :H} #{:A})))

    (t/is (= [#{:C :D :E} #{:F :G :H}]
             (ghd/separate h #{:A :B :C :D :E :F :G :H} #{:A :B})))

    (t/is (= [#{:E}]
             (ghd/separate h #{:C :D :E} #{:C :D})))

    (t/is (= [#{:F :H}]
             (ghd/separate h #{:F :G :H} #{:G})))))

(t/deftest can-compute-join-order
  (let [h (ghd/->hgraph backtracking-paper-graph)]
    (t/is (= '[a b c d e f g i h j]
             (ghd/htree-join-order
              (ghd/det-k-decomp h 2))))))

(t/deftest can-decompose-using-backtracking
  (let [h (ghd/->hgraph backtracking-paper-graph)
        ht (ghd/det-k-decomp h 2)]

    ;; Fig. 6. A procedure call sequence and the corresponding
    ;; hypertree decomposition

    ;; :decomp-cov #{:A :B :C :D :E :F :G :H} #{}
    ;; :decomp-add #{:A :B :C :D :E :F :G :H} #{} #{}
    ;; :decomp-sub (#{:B :C :D :E :F :G :H}) #{:A}
    ;; :decomp-cov #{:B :C :D :E :F :G :H} #{a b c}
    ;; :decomp-add #{:B :C :D :E :F :G :H} #{a b c} #{:A}
    ;; :decomp-sub (#{:C :D :E} #{:F :G :H}) #{:A :B}
    ;; :decomp-cov #{:C :D :E} #{a c d f}
    ;; :decomp-add #{:C :D :E} #{a c d f} #{:C :D}
    ;; :decomp-sub (#{:E}) #{:C :D}
    ;; :decomp-cov #{:E} #{g i}
    ;; :decomp-cov #{:F :G :H} #{a b e}
    ;; :decomp-add #{:F :G :H} #{a b e} #{:A :F}
    ;; :decomp-sub (#{:G :H}) #{:A :F}
    ;; :decomp-cov #{:G :H} #{a e h}

    (t/is (= (ghd/->HTree
              #{:A} '#{a b c}
              [(ghd/->HTree
                #{:A :B} '#{a b c d e f},
                [(ghd/->HTree
                  #{:C :D} '#{a c d f g i}
                  [(ghd/->HTree #{:E} '#{g i} [])])
                 (ghd/->HTree
                  #{:A :F} '#{a b e h}
                  [(ghd/->HTree #{:G :H} '#{a e h j} [])])])])
             ht))

    (t/is (= '[a b c d e f
               g i h
               j]
             (ghd/htree-join-order ht)))
    (t/is (ghd/htree-complete? ht))
    (t/is (not= (ghd/htree-normalize ht) ht))
    (t/is (= 4 (ghd/htree-decomp-depth ht)))
    (t/is (= 2 (ghd/htree-decomp-width ht)))

    (t/is (= 3 (ghd/htree-decomp-depth (ghd/det-k-decomp h 3))))
    (t/is (not= (ghd/det-k-decomp h 2) (ghd/det-k-decomp h 3)))
    (t/is (= 3 (ghd/htree-decomp-width (ghd/det-k-decomp h 3))))

    (t/is (nil? (ghd/det-k-decomp h 1)))))

(t/deftest can-compute-cover
  (let [h (ghd/->hgraph backtracking-paper-graph)]
    (t/is (= [#{:A :F} #{:A :B} #{:A :G} #{:F :D} #{:F :H}]
             (ghd/cover h '#{a b e})))))

(def adder-15 '[[:and15 TempHa15 I29 I30]
                [:xorA13 TempG13 S13 C12]
                [:or14 C14 TempHb14 TempHa14]
                [:init C0]
                [:xorA3 S3 C2 TempG3]
                [:or12 TempHb12 TempHa12 C12]
                [:andA4 C3 TempHb4 TempG4]
                [:xorA14 TempG14 S14 C13]
                [:and13 I26 TempHa13 I25]
                [:and9 TempHa9 I17 I18]
                [:or3 C3 TempHa3 TempHb3]
                [:xorA12 S12 C11 TempG12]
                [:xor11 I21 TempG11 I22]
                [:or7 TempHb7 C7 TempHa7]
                [:andA9 C8 TempHb9 TempG9]
                [:or5 TempHb5 TempHa5 C5]
                [:or15 C15 TempHb15 TempHa15]
                [:xorA5 TempG5 C4 S5]
                [:xor14 I27 I28 TempG14]
                [:xor3 I5 I6 TempG3]
                [:or11 C11 TempHa11 TempHb11]
                [:xorA9 TempG9 C8 S9]
                [:and6 TempHa6 I12 I11]
                [:andA6 TempG6 TempHb6 C5]
                [:xorA8 S8 C7 TempG8]
                [:and10 I19 I20 TempHa10]
                [:or13 TempHb13 TempHa13 C13]
                [:andA3 C2 TempHb3 TempG3]
                [:and3 TempHa3 I5 I6]
                [:andA7 TempHb7 C6 TempG7]
                [:xorA7 TempG7 C6 S7]
                [:and8 I16 TempHa8 I15]
                [:xorA15 C14 S15 TempG15]
                [:and5 I9 I10 TempHa5]
                [:or1 TempHa1 TempHb1 C1]
                [:or8 TempHa8 TempHb8 C8]
                [:xor12 I23 I24 TempG12]
                [:xor8 TempG8 I16 I15]
                [:andA13 TempG13 TempHb13 C12]
                [:or6 TempHb6 C6 TempHa6]
                [:xor6 TempG6 I12 I11]
                [:xorA10 TempG10 S10 C9]
                [:or4 C4 TempHa4 TempHb4]
                [:xorA1 C0 S1 TempG1]
                [:xorA4 C3 S4 TempG4]
                [:and12 I23 I24 TempHa12]
                [:andA5 TempG5 TempHb5 C4]
                [:and7 I14 I13 TempHa7]
                [:andA1 C0 TempG1 TempHb1]
                [:xorA6 S6 TempG6 C5]
                [:andA8 TempHb8 TempG8 C7]
                [:xor4 TempG4 I8 I7]
                [:and4 I8 I7 TempHa4]
                [:and11 I22 TempHa11 I21]
                [:andA11 TempHb11 C10 TempG11]
                [:xor1 TempG1 I2 I1]
                [:andA15 TempG15 TempHb15 C14]
                [:or9 TempHb9 TempHa9 C9]
                [:and1 TempHa1 I1 I2]
                [:xor2 I3 I4 TempG2]
                [:xor7 TempG7 I14 I13]
                [:and14 I28 TempHa14 I27]
                [:xor5 I10 I9 TempG5]
                [:xor9 I18 I17 TempG9]
                [:andA2 TempHb2 C1 TempG2]
                [:or10 C10 TempHb10 TempHa10]
                [:andA12 TempG12 C11 TempHb12]
                [:andA10 TempHb10 C9 TempG10]
                [:andA14 TempG14 C13 TempHb14]
                [:xorA11 C10 TempG11 S11]
                [:xor15 TempG15 I29 I30]
                [:xor10 I20 TempG10 I19]
                [:or2 TempHb2 TempHa2 C2]
                [:xorA2 C1 TempG2 S2]
                [:xor13 TempG13 I26 I25]
                [:and2 I4 I3 TempHa2]])

;; NOTE: it should be possible to calculate a width of 2 here. A tree
;; with this width is returned at 3 (without internal width check),
;; but not sure if this tree is correct and how it should really
;; behave? Likely a bug somewhere, the internal width check shouldn't
;; be necessary.
(t/deftest can-decompose-adder-15
  (let [ht (first (for [n (range)
                        :let [ht (ghd/det-k-decomp (ghd/->hgraph adder-15) (inc n))]
                        :when ht]
                    ht))]
    (t/is (= 16 (ghd/htree-decomp-width ht)))
    (t/is (ghd/htree-complete? ht))))
