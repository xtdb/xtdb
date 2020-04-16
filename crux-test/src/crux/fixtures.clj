(ns ^:no-doc crux.fixtures
  (:require [clojure.test.check.clojure-test :as tcct]
            [crux.api :as api]
            [crux.io :as cio]
            [crux.tx :as tx])
  (:import crux.api.ICruxAPI
           java.util.UUID
           [java.io File]
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute))

(defn with-silent-test-check [f]
  (binding [tcct/*report-completion* false]
    (f)))

(defn maps->tx-ops
  ([maps]
   (vec (for [m maps]
          [:crux.tx/put m])))
  ([maps ts]
   (vec (for [m maps]
          [:crux.tx/put m ts]))))

(defn transact!
  "Helper fn for transacting entities"
  ([api entities]
   (transact! api entities (cio/next-monotonic-date)))
  ([^ICruxAPI api entities ts]
   (doto (api/submit-tx api (maps->tx-ops entities ts))
     (->> (api/await-tx api)))
   entities))

(defn entities->delete-tx-ops [entities ts]
  (vec (for [e entities]
         [:crux.tx/delete e ts])))

(defn delete-entities!
  ([api entities]
   (delete-entities! api entities (cio/next-monotonic-date)))
  ([api entities ts]
   (let [submitted-tx (api/submit-tx api (entities->delete-tx-ops entities ts))]
     (api/await-tx api submitted-tx))
   entities))

(defn random-person [] {:crux.db/id (UUID/randomUUID)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 100)
                        :salary    (rand-int 100000)})

(defn people [people-mixins]
  (->> people-mixins (map merge (repeatedly random-person))))

(defn with-tmp-dir* [prefix f]
  (let [dir (.toFile (Files/createTempDirectory prefix (make-array FileAttribute 0)))]
    (try
      (f dir)
      (finally
        (cio/delete-dir dir)))))

(defmacro with-tmp-dir [prefix [dir-binding] & body]
  `(with-tmp-dir* ~prefix (fn [~(-> dir-binding (with-meta {:type File}))]
                            ~@body)))
