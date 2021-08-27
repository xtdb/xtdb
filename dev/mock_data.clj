(ns mock-data
  (:require dev
            [clojure.edn :as edn]
            [clojure.string :as string]
            [xtdb.api :as xt]))

(defn read-doc
  [doc-title]
  (edn/read-string (slurp (str "dev/mock-data/" doc-title))))


(def employees (read-doc "employees.edn"))
(def customers (read-doc "customers.edn"))
(def addresses (read-doc "addresses.edn"))
(def cars (read-doc "cars.edn"))

(def employee-count 80)
(def customer-count 200)

(defn get-n
  [n strs data]
  (apply concat
         (map
          (fn [x]
            (take n (filter #(= x (:position %)) data))) strs)))

(defn link-address
  [data addresses]
  (map (fn [x address]
         (assoc x :address
                (:xt/id address))) data addresses))

(def final-employees
  (let [top-management (get-n 1 ["CEO" "CTO" "COO" "CMO"] employees)
        regular-employees (take (- employee-count 4)
                                (remove
                                 (fn [x]
                                   (some #(= % (:position x)) ["CEO" "CTO" "COO" "CMO"]))
                                 employees))
        full-employees (concat top-management regular-employees)]
    (link-address full-employees (take employee-count addresses))))

(def final-customers (link-address
                      (take customer-count customers)
                      (drop employee-count addresses)))

(def final-address (take (+ customer-count employee-count) addresses))

(def final-cars (take 200 cars))

(def final-sales
  (let [sales-employees (filter (every-pred
                                 #(not (string/includes? (:position %) "Engineer"))
                                 #(string/includes? (:position %) "Sales"))
                                final-employees)]
    (map (fn [{:keys [xt/id]}]
           {:xt/id (java.util.UUID/randomUUID)
            :car (:xt/id (rand-nth final-cars))
            :type :sale
            :employee (:xt/id (rand-nth sales-employees))
            :customer id
            :status (rand-nth ["Shipped" "Processed" "Delivered"])}) final-customers)))

(defn ingest-data
  [data]
  (xt/submit-tx
   (dev/crux-node)
   (mapv #(do [::xt/put %]) data)))

(comment
  (ingest-data
   (concat final-employees final-customers final-address final-cars final-sales)))
