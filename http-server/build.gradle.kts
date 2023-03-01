plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 HTTP Server")
            description.set("Core2 HTTP Server")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))
    api(project(":wire-formats"))

    api("ring", "ring-core", "1.9.4")
    api("info.sunng", "ring-jetty9-adapter", "0.15.2")
    api("org.eclipse.jetty", "jetty-alpn-server", "10.0.6")

    api("metosin", "muuntaja", "0.6.8")
    api("metosin", "jsonista", "0.3.3")
    api("metosin", "reitit-core", "0.5.15")
    api("metosin", "reitit-interceptors", "0.5.15")
    api("metosin", "reitit-ring", "0.5.15")
    api("metosin", "reitit-http", "0.5.15")
    api("metosin", "reitit-sieppari", "0.5.15")
    api("metosin", "reitit-swagger", "0.5.15")
    api("metosin", "reitit-spec", "0.5.15")

    api("com.cognitect", "transit-clj", "1.0.329")
    api("com.widdindustries", "time-literals", "0.1.6")
}
