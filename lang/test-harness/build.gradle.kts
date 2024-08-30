import org.gradle.api.tasks.JavaExec

plugins {
    `java-library`
    id("dev.clojurephant.clojure")
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
    devImplementation("integrant", "integrant", "0.8.0")
    devImplementation("integrant", "repl", "0.3.2")
}

val defaultJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

tasks.javadoc.get().enabled = false

tasks.withType<Test> {
    // Always run the tests
    // Otherwise changes to the python code don't trigger the tests
    outputs.upToDateWhen { false }
}

tasks.register<JavaExec>("httpProxy") {
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("clojure.main")
    this.args = listOf("-m", "test-harness.core")
    jvmArgs = defaultJvmArgs
}
