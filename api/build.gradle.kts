plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB API")
            description.set("XTDB API")
        }
    }
}

dependencies {
    compileOnlyApi(files("src/main/resources"))
    implementation("org.clojure", "clojure", "1.11.1")
    api("org.clojure", "spec.alpha", "0.3.218")

    api("com.widdindustries", "time-literals", "0.1.10")
    api("com.cognitect", "transit-clj", "1.0.329")

    implementation("org.apache.arrow", "arrow-algorithm", "14.0.0")
    implementation("org.apache.arrow", "arrow-compression", "14.0.0")
    implementation("org.apache.arrow", "arrow-vector", "14.0.0")
    implementation("org.apache.arrow", "arrow-memory-netty", "14.0.0")
}

tasks.javadoc {
    exclude("xtdb/IResultSet", "xtdb/util/*")
}
