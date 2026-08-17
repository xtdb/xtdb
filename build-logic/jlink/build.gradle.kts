plugins {
    java
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

// One JRE serves all four docker variants, so the list is the *union* of what each needs
// and regenerating it means running jdeps over every variant's jar. Checking one is what
// hid the missing jdk.net (aws-only) until an S3 node fell over on it — see #5903.
//
//   ./gradlew :docker:standalone:shadowJar :docker:aws:shadowJar :docker:azure:shadowJar :docker:google-cloud:shadowJar
//   for v in standalone aws azure google-cloud; do
//     jdeps --ignore-missing-deps --multi-release 21 --print-module-deps \
//       docker/$v/build/libs/xtdb-$v.jar
//   done
//
// jdeps is static analysis, so it reports a module whether or not the referencing code is
// ever entered — take everything it names, because "that path looks dead" is a judgement
// the next dependency bump can quietly invalidate.
val jlinkModules = listOf(
    // from jdeps
    "java.base", "java.compiler", "java.desktop", "java.instrument",
    "java.naming", "java.prefs", "java.rmi", "java.scripting",
    "java.security.jgss", "java.security.sasl", "java.sql",
    "jdk.httpserver", "jdk.management", "jdk.unsupported",

    // jdk.net: HttpClient5's DefaultHttpClientConnectionOperator initialises jdk.net.Sockets.
    // Reached on an S3 node through the *sync* client the credential chain builds (STS web
    // identity on EKS), never through S3AsyncClient — which is why every test, all of which
    // supply static credentials, sails past it.
    "jdk.net",

    // manual additions
    "java.logging",
    "java.net.http",
    "jdk.crypto.ec",
    "jdk.crypto.cryptoki",
    // -PdebugJvm, which needs the jdk.jdwp.agent that jdk.jdi pulls in
    "jdk.jdi",
    // clojureRepl
    "jdk.compiler",
    "jdk.javadoc",

    // production diagnostics. Without these a shipped image cannot be asked what it's
    // doing, which is how #5850 got as far as it did with no thread dump.
    // jcmd/jstack attach by signalling the target, so they're no use where the security
    // context blocks signals to the JVM's pid — jfr is the signal-free path, via
    // -XX:StartFlightRecording at JVM start.
    "jdk.jcmd",
    "jdk.jfr",
)

val customJreDir = layout.buildDirectory.dir("custom-jre")

val toolchainLauncher = extensions.getByType(JavaToolchainService::class.java)
    .launcherFor(java.toolchain)

tasks.register<Exec>("buildCustomJre") {
    description = "Build a custom JRE using jlink"
    val outputDir = customJreDir.get().asFile
    val jlinkPath = toolchainLauncher.map {
        it.metadata.installationPath.file("bin/jlink").asFile.absolutePath
    }

    inputs.property("modules", jlinkModules.joinToString(","))
    inputs.property("jlinkPath", jlinkPath)
    outputs.dir(outputDir)

    doFirst { outputDir.deleteRecursively() }

    commandLine(
        jlinkPath.get(),
        "--add-modules", jlinkModules.joinToString(","),
        "--strip-debug", "--no-man-pages", "--no-header-files",
        "--compress=zip-6",
        "--output", outputDir.absolutePath
    )
}
