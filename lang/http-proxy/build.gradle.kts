import xtdb.DataReaderTransformer

plugins {
    java
    application
    id("dev.clojurephant.clojure")
    id("com.github.johnrengelman.shadow")
}

dependencies {
    implementation(project(":xtdb-api"))
    implementation(project(":xtdb-core"))
    implementation(project(":xtdb-http-server"))

    implementation("ring", "ring-core", "1.10.0")
    implementation("info.sunng", "ring-jetty9-adapter", "0.22.4")
    implementation("org.eclipse.jetty", "jetty-alpn-server", "10.0.15")
    implementation("metosin", "reitit-core", "0.5.15")
    implementation("pro.juxt.clojars-mirrors.integrant", "integrant", "0.8.0")

    testImplementation("pro.juxt.clojars-mirrors.hato", "hato", "0.8.2")
    // hato uses cheshire for application/json encoding
    testImplementation("cheshire", "cheshire", "5.12.0")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc.get().enabled = false

application {
    mainClass.set("clojure.main")
}

tasks.shadowJar {
    archiveBaseName.set("http-proxy")
    archiveVersion.set("")
    archiveClassifier.set("")
    mergeServiceFiles()
    transform(DataReaderTransformer())
}
