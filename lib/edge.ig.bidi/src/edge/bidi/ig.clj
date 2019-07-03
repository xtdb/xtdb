(ns edge.bidi.ig
  (:require
    bidi.vhosts
    [integrant.core :as ig]))

;; Workaround for https://github.com/juxt/bidi/issues/187 pending a merge of https://github.com/juxt/bidi/pull/196
(defn- fix-vhost
  "Remove coercions to the route structures"
  [vhost input]
  (update vhost :vhosts
          (fn [vhosts]
            (mapv
              vector
              (map first vhosts)
              (map second input)))))

(defmethod ig/init-key ::vhost
  [_ args]
  (fix-vhost (apply bidi.vhosts/vhosts-model args) args))
