(ns crux-ui-server.generate-web-manifest)

(defn generate-str [{:console/keys [routes-prefix] :as conf}]
  (str  "{
  \"name\"                : \"Crux Console\",
  \"short_name\"          : \"Crux Console\",
  \"start_url\"           : \"" routes-prefix "/app\",
  \"display\"             : \"standalone\",
  \"theme_color\"         : \"hsl(32, 91%, 54%)\",
  \"background_color\"    : \"rgb(167, 191, 232)\",
  \"description\"         : \"Console for Crux to see data in perspectives\",
  \"icons\"               : [{
    \"src\"               : \"" routes-prefix "/static/img/cube-on-white-120.png\",
    \"sizes\"             : \"120x120\",
    \"type\"              : \"image/png\"
  }, {
    \"src\"               : \"" routes-prefix "/static/img/cube-on-white-192.png\",
    \"sizes\"             : \"192x192\",
    \"type\"              : \"image/png\"
  }, {
    \"src\"               : \"" routes-prefix "/static/img/cube-on-white-512.png\",
    \"sizes\"             : \"512x512\",
    \"type\"              : \"image/png\"
  }]
}"))
