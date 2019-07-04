(ns juxt.crux-ui.frontend.logging)

(defn debug [& values]
  (apply (.-debug js/console) (map clj->js values)))

(defn log [& values]
  (apply (.-log js/console) (map clj->js values)))

(defn info [& values]
  (apply (.-info js/console) (map clj->js values)))

(defn warn [& values]
  (apply (.-warn js/console) (map clj->js values)))

(defn error [& values]
  (apply (.-error js/console) (map clj->js values)))