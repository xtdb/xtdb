group = "juxt"
version = "0.1.0-SNAPSHOT"

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.6.0-beta.1"
}

repositories {
    jcenter()
    maven { name = "Clojars"; url = uri("https://repo.clojars.org") }
}

dependencies {
    implementation("org.clojure", "clojure", "1.10.2")
    implementation("org.clojure", "tools.logging", "1.1.0")
    implementation("org.apache.arrow", "arrow-vector", "3.0.0")
    implementation("org.apache.arrow", "arrow-memory-netty", "3.0.0")
    implementation("org.roaringbitmap", "RoaringBitmap", "0.9.3")

    testImplementation("org.clojure", "data.csv", "1.0.0")

    devImplementation("nrepl", "nrepl", "0.6.0")
    devImplementation("cider:cider-nrepl:0.25.2")
}

sourceSets {
    main {
        resources.srcDir("src")
        resources.srcDir("resources")
    }
    test {
        resources.srcDir("data")
        resources.srcDir("test")
    }
}

tasks.test {
    // Use junit platform for unit tests.
    useJUnitPlatform()
}

tasks.clojureRepl {
    handler.set("cider.nrepl/cider-nrepl-handler")
}
