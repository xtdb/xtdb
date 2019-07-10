(ns juxt.crux-ui.frontend.svg-icons)

(defn- icon [icon-name svg]
  [:div.svg-icon {:class (str "svg-icon--" (name icon-name))}
   svg])

(def close
  (icon :close
    [:svg.svg-icon__content {:viewBox "0 0 1000 1000" :xmlns "http://www.w3.org/2000/svg"}
     [:path {:d "M1008 898.915l-45.085 45.085-450.915-450.915-450.915 450.915-45.085-45.085 450.915-450.915-450.915-450.915 45.085-45.085 450.915 450.915 450.915-450.915 45.085 45.085-450.915 450.915z"}]]))


(def styles
  [[:.svg-icon
    {:width :12px
     :height :12px
     :color  :grey}
    [:&__content
     {:width :100%
      :height :100%}]]])
