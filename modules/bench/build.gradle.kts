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

    api(libs.clojure.data.csv)
    api(libs.clojure.test.check)

    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jpl)

    // bench
    api(libs.oshi.core)
    api(libs.tufte)
    api(libs.hiccup)
    api(libs.testcontainers)
    api(libs.kixi.stats)

    api(libs.aws.s3)
    api(libs.clj.http)
}


tasks.shadowJar {
    transform(DataReaderTransformer())
    archiveFileName.set("bench-standalone.jar")
    setProperty("zip64", true)
    mergeServiceFiles()
    transform(DataReaderTransformer())
}
