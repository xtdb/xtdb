(ns xtdb.bench.food-prices-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.bench.food-prices :as fp]
            [xtdb.test-util :as tu :refer [*node*]])
  (:import (java.time LocalDate ZoneOffset)))

(t/use-fixtures :each tu/with-node)

(defn- prices [month & specs]
  (for [[place-id product-id price] specs]
    {"place_id" place-id, "product_id" product-id, "um_id" 1
     "month" (LocalDate/parse month)
     ;; derived, so an unchanged price leaves both compared columns unchanged
     "price" price, "price_usd" (/ price 2.0)}))

(def ^:private dims
  [{:table "place_info", :fk "place_id", :rows [{"id" 0, "name" "Ankara"} {"id" 1, "name" "Izmir"}]}
   {:table "product_info", :fk "product_id", :rows [{"id" 1, "name" "Rice"} {"id" 2, "name" "Bulgur"}]}
   {:table "unit_info", :fk "um_id", :rows [{"id" 1, "name" "KG"}]}])

(def ^:private partitions
  ;; three monthly restatements; (0,2) doesn't move in February and (0,1) doesn't in March
  [(prices "2013-01-01" [0 1 10.0] [0 2 20.0] [1 1 30.0])
   (prices "2013-02-01" [0 1 11.0] [0 2 20.0] [1 1 31.0])
   (prices "2013-03-01" [0 1 11.0] [0 2 21.0] [1 1 32.0])])

(defn- ->instant [date]
  (.toInstant (.atStartOfDay (LocalDate/parse date) ZoneOffset/UTC)))

(deftest only-restatements-that-move-the-price-are-written
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (= {:partitions 3, :written 7}
             (fp/ingest! conn dims {} partitions))
          "two of the nine incoming rows repeat a held price, so they're never written")

    (t/is (= 7 (:versions (first (xt/q conn "SELECT COUNT(*) AS versions
                                             FROM food_prices FOR ALL VALID_TIME"))))
          "and leave no version behind either — one row-version per write, not per row seen")))

(deftest each-partition-is-visible-at-its-own-valid-time
  (with-open [conn (xt/open-adbc-conn *node*)]
    (fp/ingest! conn dims {} partitions)

    (t/is (= [{:place-id 0, :product-id 1, :price 11.0}
              {:place-id 0, :product-id 2, :price 21.0}
              {:place-id 1, :product-id 1, :price 32.0}]
             (xt/q conn "SELECT p.place_id, p.product_id, p.price FROM food_prices AS p
                         ORDER BY p.place_id, p.product_id"))
          "the latest basis holds March's prices")

    ;; each batch is published a month in arrears, so February's landed on 2013-03-01
    (t/is (= [{:place-id 0, :product-id 1, :price 11.0}
              {:place-id 0, :product-id 2, :price 20.0}
              {:place-id 1, :product-id 1, :price 31.0}]
             (xt/q conn ["SELECT p.place_id, p.product_id, p.price
                          FROM food_prices FOR VALID_TIME AS OF ? AS p
                          ORDER BY p.place_id, p.product_id"
                         (->instant "2013-03-15")])
             )
          "mid-March still holds February's, including the price that never moved")))

(deftest foreign-keys-are-resolved-once-and-reused
  (with-open [conn (xt/open-adbc-conn *node*)]
    (fp/ingest! conn dims {} partitions)

    (t/is (= [{:versions 2} {:versions 2} {:versions 1}]
             (for [table ["place_info" "product_info" "unit_info"]]
               (first (xt/q conn (format "SELECT COUNT(*) AS versions
                                          FROM %s FOR ALL VALID_TIME" table)))))
          "every partition re-resolves the dimensions, but only the first inserts them")))

(deftest composite-keys-round-trip-through-the-library-encoding
  (with-open [conn (xt/open-adbc-conn *node*)]
    (fp/ingest! conn dims {} partitions)

    (t/is (= [{:xt/id (fp/->pk {"place_id" 0, "product_id" 1, "um_id" 1})}]
             (xt/q conn "SELECT p._id FROM food_prices AS p
                         WHERE p.place_id = 0 AND p.product_id = 1"))
          "the row is addressed by the encoded composite key, not a surrogate")))
