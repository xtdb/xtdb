group = "juxt"
version = "0.1.0-SNAPSHOT"

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.6.0"
}

repositories {
    jcenter()
    maven { name = "Clojars"; url = uri("https://repo.clojars.org") }
}

dependencies {
    implementation("org.clojure", "clojure", "1.10.2")
    implementation("org.clojure", "tools.logging", "1.1.0")
    implementation("org.apache.arrow", "arrow-vector", "3.0.0")
    implementation("org.apache.arrow", "arrow-algorithm", "3.0.0")
    implementation("org.apache.arrow", "arrow-memory-netty", "3.0.0")
    implementation("org.roaringbitmap", "RoaringBitmap", "0.9.8")

    testImplementation("org.clojure", "data.csv", "1.0.0")
    testRuntimeOnly("org.ajoberstar", "jovial", "0.3.0")

    devImplementation("nrepl", "nrepl", "0.6.0")
    devImplementation("cider", "cider-nrepl", "0.25.8")
}

sourceSets {
    remove(dev.get())

    main {
        java.srcDirs.clear()
        java.srcDir("src")

        resources.srcDirs.clear()
        resources.srcDir("src").removeAll { it.endsWith(".java") }
        resources.srcDir("resources")
    }
    test {
        java.srcDirs.clear()
        java.srcDir("test")

        resources.srcDirs.clear()
        resources.srcDir("data")
        resources.srcDir("test").removeAll { it.endsWith(".java") }
    }
}

tasks.test {
    // Use junit platform for unit tests.
    useJUnitPlatform()
}

tasks.clojureRepl {
    handler.set("cider.nrepl/cider-nrepl-handler")
}
