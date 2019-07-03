(ns juxt.crux-ui.frontend.views.cluster-health)


(def cluster-mock
  (let [protonode {:crux.version/version "19.04-1.0.1-alpha",
                   :crux.kv/size 792875,
                   :crux.index/index-version 4,
                   :crux.tx-log/consumer-state
                   {:crux.tx/event-log
                    {:lag 0, :next-offset 1592957991111682,
                     :time #inst "2019-04-18T21:30:38.195-00:00"}}}
        protonode2 {:crux.version/version "19.04-1.0.0-alpha",
                    :crux.kv/size 792875,
                    :crux.index/index-version 4,
                    :crux.tx-log/consumer-state
                    {:crux.tx/event-log
                     {:lag 0, :next-offset 1592957988161759,
                      :time #inst "2019-04-18T21:15:12.561-00:00"}}}]
    [["http://node-1.crux.cloud:8080" [:span "Status: OK " [:span {:style {:font-weight "bold" :color "green"}} "✓"]] protonode]
     ["http://node-2.crux.cloud:8080" [:span "Status: OK " [:span {:style {:font-weight "bold" :color "green"}} "✓"]] protonode]
     ["http://node-3.crux.cloud:8080" [:span "Error: " [:span {:style {:font-weight "normal" :color "red"}} "Connection Timeout ✘"]] protonode2]]))

(defn root []
  [:div.cluster-health
   [:h2 "Cluster Health"]
   [:div.cg {:style {:display "grid"
                     :margin-right "auto"
                     :margin-left "auto"
                     :grid-template-columns "2em auto auto auto"
                     :grid-row-gap "1em"
                     }}
      (mapcat identity
              (for [n cluster-mock]
                [
                 [:div {:key (str n 0)} [:a {:style {:text-decoration "none" :font-weight "bold" :font-size "1.3em"}} "↻"]]
                 [:div {:key (str n 1)}[:a (nth n 0)]]
                 [:div {:key (str n 2)}(nth n 1)]
                 [:div {:key (str n 3)}[:pre (with-out-str (cljs.pprint/pprint (nth n 2)))]]
                 ]
                ))

    [:div {:style {:height "1em"}}]
    [:a "Refresh All"]]])
