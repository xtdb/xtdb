(ns edge.yada.ig
  (:require
    [clojure.java.io :as io]
    [edge.system.meta :refer [useful-info]]
    [yada.yada :as yada]
    [integrant.core :as ig]
    [yada.resources.resources-resource :refer [new-resources-resource]]))

(defmethod ig/init-key ::listener
  [_ opts]
  (assoc (yada/listener (:handler opts)
                        (dissoc opts :handler))
         ::handler (:handler opts)))

(defmethod ig/halt-key! ::listener
  [_ {:keys [close]}]
  (when close (close)))

(defmethod ig/init-key ::redirect
  [_ {:keys [target opts]}]
  (apply yada/redirect (filter some? [target opts])))

(defmethod ig/init-key ::resources
  [_ {:keys [path id]}]
  (cond-> (new-resources-resource path)
    id
    (assoc :id id)))

(defmethod ig/init-key ::classpath-name
  [_ {:keys [name]}]
  (yada/as-resource (io/resource name)))

(defmethod ig/init-key ::webjar
  [_ {:keys [webjar] :as options}]
  (@(requiring-resolve 'yada.resources.webjar-resource/new-webjar-resource)
   webjar
   options))

(defmethod ig/init-key ::webjars-route-pair
  [_ options]
  (@(requiring-resolve 'yada.resources.webjar-resource/webjars-route-pair)
   options))

;; Use getName to avoid requiring a direct dependency on bidi, etc.
(defmulti ^:private hosts
  (fn [config state]
    (some-> (::handler state) type (.getName))))

(defmethod hosts "bidi.vhosts.VHostsModel"
  [config state]
  (let [vhosts (mapcat first (:vhosts (::handler state)))]
    (map (fn [vhost]
           (if (= :* vhost)
             (str "http://localhost:" (:port state))
             (str (name (:scheme vhost)) "://" (:host vhost))))
         vhosts)))

(defmethod hosts :default
  [config state]
  ;; Not a terrible assumption
  [(str "http://localhost:" (:port state))])

(defmethod useful-info ::listener
  [_ config state]
  (str "Website listening on: "
       (apply str (interpose " " (hosts config state)))))
