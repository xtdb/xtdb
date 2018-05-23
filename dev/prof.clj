(ns prof
  (:require [clojure.reflect]))

(set! *unchecked-math* :warn-on-boxed)

(def profile-state (atom {}))

(defn profile-fn [f & args]
  (let [start-time (System/currentTimeMillis)]
    (try
      (apply f args)
      (finally
        (swap! profile-state update-in [f]
               (fn [{:keys [^long n ^long t]
                     :or {n 0 t 0}}]
                 (let [t (+ t (- (System/currentTimeMillis) start-time))
                       n (inc n)]
                   {:t t
                    :n n
                    :a (double (/ t n))})))))))

(defn primitive-fn? [f]
  (->>  (:members (clojure.reflect/reflect f))
        (some (comp #{'invokePrim} :name))))

(defn profile! [& ns]
  (reset! profile-state {})
  (doseq [ns ns
          [_ v] (ns-interns ns)]
    (alter-var-root v #(if (and (fn? %)
                                (not (primitive-fn? %)))
                         (partial profile-fn %)
                         %))))
