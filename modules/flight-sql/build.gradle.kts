plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    `maven-publish`
    signing
}

publishing {
    publications.create("maven", MavenPublication::class) {
        pom {
            name.set("Core2 FlightSQL Server")
            description.set("Core2 FlightSQL Server")
        }
    }
}

dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.apache.arrow", "arrow-vector", "11.0.0")
    api("org.apache.arrow", "flight-sql", "11.0.0")
}
