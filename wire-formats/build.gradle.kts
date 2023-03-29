plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB Wire Formats")
            description.set("XTDB Wire Formats")
        }
    }
}

dependencies {
    api(project(":api"))
    compileOnlyApi(files("src/main/resources"))

    api("org.clojure", "clojure", "1.11.1")
    api("com.widdindustries", "time-literals", "0.1.10")
    api("com.cognitect", "transit-clj", "1.0.329")

    api("org.apache.arrow", "arrow-algorithm", "11.0.0")
    api("org.apache.arrow", "arrow-compression", "11.0.0")
    api("org.apache.arrow", "arrow-vector", "11.0.0")
    api("org.apache.arrow", "arrow-memory-netty", "11.0.0")
}
