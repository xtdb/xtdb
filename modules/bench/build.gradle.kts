import xtdb.DataReaderTransformer

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    id("com.gradleup.shadow")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

dependencies {
    api(project(":"))
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":modules:xtdb-datasets"))
    api(project(":modules:xtdb-kafka"))
    api(project(":modules:xtdb-aws"))
    api(project(":modules:xtdb-azure"))
    api(project(":modules:xtdb-google-cloud"))

    api("org.clojure", "data.csv", "1.0.1")

    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jpl)

    // bench
    api("com.github.oshi", "oshi-core", "6.3.0")
    api("pro.juxt.clojars-mirrors.hiccup", "hiccup", "2.0.0-alpha2")

    api("software.amazon.awssdk", "s3", "2.25.24")
}


tasks.shadowJar {
    transform(DataReaderTransformer())
    archiveFileName.set("bench-standalone.jar")
    setProperty("zip64", true)
    mergeServiceFiles()
    transform(DataReaderTransformer())
}
