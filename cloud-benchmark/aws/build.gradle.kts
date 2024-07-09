import xtdb.DataReaderTransformer

plugins {
    java
    application
    id("com.github.johnrengelman.shadow")
}

dependencies {
    implementation(project(":cloud-benchmark"))
    implementation(project(":modules:xtdb-aws"))
    implementation("ch.qos.logback", "logback-classic", "1.4.5")
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

application {
    mainClass.set("clojure.main")
}

tasks.shadowJar {
    archiveBaseName.set("xtdb")
    archiveVersion.set("")
    archiveClassifier.set("aws-bench")
    mergeServiceFiles()
    transform(DataReaderTransformer())
}
