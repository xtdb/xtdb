import xtdb.DataReaderTransformer

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
    id("com.github.johnrengelman.shadow")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(17))

dependencies {
    api(project(":xtdb-api"))
    api(project(":xtdb-core"))
    api(project(":modules:datasets"))
    api(project(":modules:xtdb-jdbc"))
    api(project(":modules:xtdb-kafka"))
    api(project(":modules:xtdb-s3"))

    api("org.clojure", "data.csv", "1.0.1")
    api("ch.qos.logback", "logback-classic", "1.4.5")

    // bench
    api("io.micrometer", "micrometer-core", "1.9.5")
    api("com.github.oshi", "oshi-core", "6.3.0")
    api("pro.juxt.clojars-mirrors.hiccup", "hiccup", "2.0.0-alpha2")
}


tasks.shadowJar {
    transform(DataReaderTransformer())
}
