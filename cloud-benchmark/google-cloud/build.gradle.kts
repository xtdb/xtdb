import xtdb.DataReaderTransformer

plugins {
    java
    application
    id("com.gradleup.shadow")
}

dependencies {
    implementation(project(":cloud-benchmark"))
    implementation(project(":modules:xtdb-google-cloud"))

    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jpl)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

application {
    mainClass.set("clojure.main")
}

tasks.shadowJar {
    archiveBaseName.set("xtdb")
    archiveVersion.set("")
    archiveClassifier.set("google-cloud-bench")
    setProperty("zip64", true)
    mergeServiceFiles()
    transform(DataReaderTransformer())
    isZip64 = true
}
