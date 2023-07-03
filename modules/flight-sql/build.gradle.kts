plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("XTDB FlightSQL Server")
            description.set("XTDB FlightSQL Server")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.apache.arrow", "arrow-vector", "12.0.1")
    api("org.apache.arrow", "flight-sql", "12.0.1")

    testImplementation(project(":"))
    testImplementation(project(":http-client-clj"))

    // brings in vendored SLF4J (but doesn't change the class names). naughty.
    // https://github.com/apache/arrow/issues/34516
    testImplementation("org.apache.arrow", "flight-sql-jdbc-driver", "12.0.1")

    testImplementation("pro.juxt.clojars-mirrors.com.github.seancorfield", "next.jdbc", "1.2.674")
}
