(ns example-standalone-webservice.main
  (:require [crux.codec :refer [index-version]]
            [crux.io :as crux-io]
            [clojure.tools.logging :as log]
            [yada.yada :refer [handler listener]]
            [hiccup2.core :refer [html]]
            [yada.resource :refer [resource]]
            [clojure.java.shell :refer [sh]]
            [example-standalone-webservice.backup :as backup])
  (:import [crux.api Crux IndexVersionOutOfSyncException]))

(defn get-handler
  [ctx {:keys [crux]}]
  (fn [ctx]
    (str
      (html
        [:div
         [:h1 "Message Board"]
         [:div
          "here you can post messages :)"]
         [:div
          [:pre "Status: " (pr-str (.status crux))]
          [:pre "Index-version:" index-version]
          [:pre (:out (sh "ls" "-lh" "data"))]
          [:pre (:out (sh "lsblk"))]
          [:pre (:out (sh "pwd"))]]
         [:br]
         [:form {:action "" :method "POST"}
          [:label "Name: "] [:br]
          [:input {:type "text" :name "name"}] [:br]
          [:br]
          [:label "Message: "] [:br]
          [:textarea {:cols "40" :rows "10" :name "message"}] [:br]
          [:input {:type "submit" :value "Submit"}]]

         [:br]
         [:div
          [:ul
           (for [[message name created]
                 (sort-by #(nth % 2)
                          (.q (.db crux)
                              '{:find [m n c]
                                :where [[e :message-post/message m]
                                        [e :message-post/name n]
                                        [e :message-post/created c]]}))]
             [:li
              [:span "From: " name] [:br]
              [:span "Created: " created] [:br]
              [:pre message]])]]]))))


(defn post-handler
  [ctx {:keys [crux]}]
  (let [{:keys [name message]} (:form (:parameters ctx))]
    (let [id (java.util.UUID/randomUUID)]
      (.submitTx
       crux
       [[:crux.tx/put
         id
         {:crux.db/id id
          :message-post/created (java.util.Date.)
          :message-post/name name
          :message-post/message message}]]))

    (assoc (:response ctx)
           :status 302
           :headers {"location" "/"})))


(defn application-resource
  [system]
  (resource
    {:methods
     {:get {:produces "text/html"
            :response #(get-handler % system)}
      :post {:consumes "application/x-www-form-urlencoded"
             :parameters {:form {:name String
                                 :message String}}
             :produces "text/html"
             :response #(post-handler % system)}}}))

(def index-dir "data/db-dir-1")
(def log-dir "data/eventlog-1")

(def crux-options {:kv-backend "crux.kv.rocksdb.RocksKv"
                   :event-log-dir log-dir
                   :db-dir index-dir})

(def backup-options {:backup-dir "backup"
                     :backend :shell
                     :shell/backup-script "bin/backup.sh"
                     :shell/restore-script "bin/restore.sh"})

(defn -main []
  (try
    (backup/check-and-restore crux-options backup-options)
    (let [port 8085]
      (with-open [crux-system (Crux/startStandaloneSystem crux-options)

                  http-server
                  (let [l (listener
                            (application-resource {:crux crux-system})
                            {:port port})]
                    (log/info "started webserver on port:" port)
                    (reify java.io.Closeable
                      (close [_]
                        ((:close l)))))]

        (while (not (.isInterrupted (Thread/currentThread)))
          (Thread/sleep 5000)
          (backup/backup-current-version crux-system crux-options backup-options))))
    (catch IndexVersionOutOfSyncException e
      (crux-io/delete-dir index-dir)
      (-main))
    (catch Exception e
      (log/error e "what happened" (ex-data e)))))
