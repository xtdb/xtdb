(ns juxt.edge.graphql-ws.core
  (:require
    [goog.object :as object]))

(defn- websocket
  ([url] (new js/WebSocket url))
  ([url protocol] (new js/WebSocket url protocol)))

(defn- send
  [ws operation-message]
  (.send ws (js/JSON.stringify (clj->js operation-message))))

(defn- on-open
  [ws cb]
  (object/set ws "onopen" cb))

(defn- on-message
  [ws cb]
  (object/set ws "onmessage" cb))

(defn- cljsify-gql-message-event
  [evt]
  (let [x (js/JSON.parse (.-data evt))]
    (js->clj x :keywordize-keys true)))

(def ^:client GQL_CONNECTION_INIT "connection_init")
(def ^:server GQL_CONNECTION_ACK "connection_ack")
(def ^:server GQL_CONNECTION_ERROR "connection_error")
(def ^:client GQL_CONNECTION_TERMINATE "connection_terminate")
(def ^:client GQL_CONNECTION_KEEP_ALIVE "connection_keep_alive")
(def ^:client GQL_START "start")
(def ^:server GQL_DATA "data")
(def ^:server GQL_ERROR "error")
(def ^:server GQL_COMPLETE "complete")
(def ^:client GQL_STOP "stop")

(defn- create-graphql-websocket
  [ws ready-fn callback-fn connection-params]
  (doto ws
    (on-open
     (fn [evt]
       (send ws (cond-> {:type GQL_CONNECTION_INIT}
                  connection-params
                  (assoc :payload connection-params)))
       (ready-fn ws)))
    (on-message
      (comp
        callback-fn
        (fn [{:keys [type id payload]}]
          (condp = type
            GQL_CONNECTION_ACK {:type :gql.ws/ack}
            GQL_CONNECTION_KEEP_ALIVE {:type :gql.ws/keep-alive}
            GQL_COMPLETE {:type :gql.ws/complete
                          :gql.ws.operation/id id}
            GQL_ERROR {:type :gql.ws/error
                       :gql.ws/error-type :failing-operation
                       :gql.ws.operation/id id
                       :gql.ws.error/errors [payload]}
            GQL_DATA (if (:errors payload)
                       {:type :gql.ws/error
                        :gql.ws/error-type :resolver-error
                        :gql.ws.operation/id id
                        :gql.ws.error/errors (:errors payload)
                        :gql.ws/data (:data payload)}
                       {:type :gql.ws/data
                        :gql.ws.operation/id id
                        :gql.ws/data (:data payload)})))
        cljsify-gql-message-event))))

(defn- gql-start
  [ws id payload]
  (send ws {:type GQL_START
            :id id
            :payload payload}))

(defn- gql-stop
  [ws id]
  (send ws {:type GQL_STOP
            :id id}))

(defn- subscribe
  [ws id {:keys [query variables operation-name]
          :as subscription-payload}]
  (gql-start ws id subscription-payload))

(defn init
  [{:keys [url connection-params subscriptions]} callback]
  {:ws
    (create-graphql-websocket (websocket url)
                              (fn ready [ws]
                                (doseq [[k v] subscriptions]
                                  (subscribe ws k v)))
                              callback
                              connection-params)
    :subscription-ids (keys subscriptions)})

(defn stop-all!
  [{:keys [ws subscription-ids]}]
  (doseq [id subscription-ids]
    (gql-stop ws id))
  (.close ws))
