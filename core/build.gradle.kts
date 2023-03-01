plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 Core")
            description.set("Core2 Core")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":wire-formats"))
    compileOnlyApi(files("src/main/resources"))

    api("org.clojure", "tools.logging", "1.2.4")
    api("org.clojure", "spec.alpha", "0.3.218")
    api("org.clojure", "data.json", "2.4.0")
    api("org.clojure", "data.csv", "1.0.1")
    api("org.clojure", "tools.cli", "1.0.206")
    api("com.cognitect", "transit-clj", "1.0.329")

    api("org.apache.arrow", "arrow-algorithm", "11.0.0")
    api("org.apache.arrow", "arrow-compression", "11.0.0")
    api("org.apache.arrow", "arrow-vector", "11.0.0")
    api("org.apache.arrow", "arrow-memory-netty", "11.0.0")
    api("io.netty", "netty-common", "4.1.82.Final")

    api("org.roaringbitmap", "RoaringBitmap", "0.9.32")

    api("pro.juxt.clojars-mirrors.integrant", "integrant", "0.8.0")

    api("org.babashka", "sci", "0.6.37")
    api("commons-codec", "commons-codec", "1.15")
}
