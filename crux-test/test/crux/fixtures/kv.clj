(ns crux.fixtures.kv
  (:require [crux.fixtures.api :as apif]
            [crux.io :as cio]))

(defn with-kv-dir [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (apif/with-opts {:crux.kv/db-dir (str db-dir)} f)
      (finally
        (cio/delete-dir db-dir)))))

(defn with-kv-backend [kv-backend f]
  (apif/with-opts {:kv-backend kv-backend} f))
