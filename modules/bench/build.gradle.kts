import xtdb.DataReaderTransformer

plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
    id("com.gradleup.shadow")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":xtdb-main"))
    api(testFixtures(project(":")))
    api(project(":modules:xtdb-datasets"))
    api(project(":modules:xtdb-kafka"))
    api(project(":modules:xtdb-aws"))
    api(project(":modules:xtdb-azure"))
    api(project(":modules:xtdb-google-cloud"))

    api("org.clojure", "data.csv", "1.0.1")
    api(libs.clojure.test.check)

    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jpl)

    // bench
    api("com.github.oshi", "oshi-core", "6.3.0")
    api("com.taoensso", "tufte", "2.6.3")
    api("pro.juxt.clojars-mirrors.hiccup", "hiccup", "2.0.0-alpha2")
    api("org.testcontainers", "testcontainers", "1.20.1")

    api("software.amazon.awssdk", "s3", "2.25.24")
    api("clj-http", "clj-http", "3.13.1")
}


tasks.shadowJar {
    transform(DataReaderTransformer())
    archiveFileName.set("bench-standalone.jar")
    setProperty("zip64", true)
    mergeServiceFiles()
    transform(DataReaderTransformer())
}
