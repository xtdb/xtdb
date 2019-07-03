(ns juxt.crux-ui.frontend.views.functions)


; bem
(defn unfold-modifier-maps [possible-modifier-map]
  (if-not (map? possible-modifier-map)
    possible-modifier-map
    (for [[k v] possible-modifier-map :when v] k)))

(defn flatten-modifiers [modifiers]
  (flatten (map unfold-modifier-maps modifiers)))

(defn- bem-str-strict [css-class-name modifiers]
  (let [base-class-name (name css-class-name)
        modifiers (flatten-modifiers modifiers)
        -append
        (fn [final-class-name modifier-name]
          (if modifier-name
            (str final-class-name " " base-class-name"--" (.replace (name modifier-name) "?" ""))
            final-class-name))]
    (reduce -append base-class-name modifiers)))

(defn bem-str [css-class-name & modifiers]
  (bem-str-strict css-class-name modifiers))

(defn bem [css-class-name & modifiers]
  {:class (bem-str-strict css-class-name modifiers)})

