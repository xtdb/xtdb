import dev.clojurephant.plugin.clojure.tasks.ClojureCompile
import org.jetbrains.dokka.base.DokkaBase
import org.jetbrains.dokka.base.DokkaBaseConfiguration
import java.time.Year

evaluationDependsOnChildren()

buildscript {
    dependencies {
        classpath("org.jetbrains.dokka:dokka-base:1.9.10")
    }
}

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.8.0-beta.7"
    id("io.freefair.aggregate-javadoc") version "6.6"
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
}

val defaultJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Djdk.attach.allowAttachSelf",
    "-Darrow.memory.debug.allocator=false",
    "-XX:-OmitStackTraceInFastThrow",
)

val sixGBJvmArgs = listOf(
    "-Xmx2g",
    "-Xms2g",
    "-XX:MaxDirectMemorySize=3g",
    "-XX:MaxMetaspaceSize=1g"
)

val twelveGBJvmArgs = listOf(
    "-Xmx5g",
    "-Xms5g",
    "-XX:MaxDirectMemorySize=6g",
    "-XX:MaxMetaspaceSize=1g"
)

java.toolchain.languageVersion.set(JavaLanguageVersion.of(21))

allprojects {
    val proj = this

    // labs sub-projects set this explicitly - this runs afterwards
    group = if (proj.hasProperty("labs")) "com.xtdb.labs" else "com.xtdb"

    version = System.getenv("XTDB_VERSION") ?: "2.0.0-SNAPSHOT"

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.clojars.org/") }
    }

    if (plugins.hasPlugin("java-library")) {
        java {
            withSourcesJar()
        }

        if (plugins.hasPlugin("org.jetbrains.dokka"))
            tasks.register<Jar>("dokkaJavadocJar") {
                dependsOn(tasks.dokkaJavadoc)
                from(tasks.dokkaJavadoc.flatMap { it.outputDirectory })
                archiveClassifier.set("javadoc")
            }

        tasks.test {
            jvmArgs(defaultJvmArgs + sixGBJvmArgs)
            useJUnitPlatform {
                excludeTags("integration", "kafka", "jdbc", "timescale", "s3", "slt", "docker", "azure", "google-cloud")
            }
        }

        tasks.create("integration-test", Test::class) {
            jvmArgs(defaultJvmArgs + twelveGBJvmArgs)
            useJUnitPlatform {
                includeTags("integration")
            }
        }

        tasks.create("nightly-test", Test::class) {
            jvmArgs(defaultJvmArgs + sixGBJvmArgs)
            useJUnitPlatform {
                includeTags("s3", "google-cloud", "azure", "kafka")
            }
        }

        dependencies {
            testRuntimeOnly("ch.qos.logback", "logback-classic", "1.4.5")

            testImplementation("org.junit.jupiter", "junit-jupiter-api", "5.8.1")
            testRuntimeOnly("org.junit.jupiter", "junit-jupiter-engine", "5.8.1")
        }

        if (plugins.hasPlugin("dev.clojurephant.clojure")) {
            dependencies {
                implementation("org.clojure", "clojure", "1.11.1")

                testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")
                nrepl("cider", "cider-nrepl", "0.47.1")
            }

            clojure {
                // disable `check` because it takes ages to start a REPL
                builds.forEach {
                    it.checkNamespaces.empty()
                }
            }

            tasks.clojureRepl {
                forkOptions.run {
                    val jvmArgs = defaultJvmArgs.toMutableList()

                    if (project.hasProperty("yourkit")) {
                        jvmArgs += "-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so=app_name=xtdb"
                    }

                    if (project.hasProperty("debugJvm")) {
                        jvmArgs += "-Xdebug"
                        jvmArgs += "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
                    }

                    if (project.hasProperty("arrowUnsafeMemoryAccess")) {
                        jvmArgs += "-Darrow.enable_unsafe_memory_access=true"
                    }

                    if (project.hasProperty("enableAssertions")) {
                        jvmArgs += "-enableassertions"
                    }

                    this.jvmArgs = jvmArgs
                }

                middleware.add("cider.nrepl/cider-middleware")
            }

            tasks.withType(ClojureCompile::class) {
                forkOptions.run {
                    jvmArgs = defaultJvmArgs
                }
            }
        }

        if (plugins.hasPlugin("maven-publish")) {
            extensions.configure(PublishingExtension::class) {
                publications.named("maven", MavenPublication::class) {
                    from(components["java"])

                    if (plugins.hasPlugin("org.jetbrains.dokka"))
                        artifact(tasks["dokkaJavadocJar"]) {
                            this.classifier = "javadoc"
                        }

                    pom {
                        url.set("https://xtdb.com")

                        licenses {
                            license {
                                name.set("The MPL License")
                                url.set("https://opensource.org/license/mpl-2-0/")
                            }
                        }
                        developers {
                            developer {
                                id.set("juxt")
                                name.set("JUXT")
                                email.set("hello@xtdb.com")
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/xtdb/xtdb.git")
                            developerConnection.set("scm:git:ssh://github.com/xtdb/xtdb.git")
                            url.set("https://xtdb.com")
                        }
                    }
                }

                repositories {
                    maven {
                        name = "ossrh"
                        val releasesRepoUrl = "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2"
                        val snapshotsRepoUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots"
                        url = uri(if (!version.toString().endsWith("-SNAPSHOT")) releasesRepoUrl else snapshotsRepoUrl)

                        credentials {
                            username = project.properties["ossrhUsername"] as? String
                            password = project.properties["ossrhPassword"] as? String
                        }
                    }
                }

                extensions.configure(SigningExtension::class) {
                    useGpgCmd()
                    sign(publications["maven"])
                }
            }
        }
    }
}

project(":xtdb-core").run {
    tasks["sourcesJar"].dependsOn("generateGrammarSource")
    tasks["dokkaJavadoc"].dependsOn("generateGrammarSource")
    tasks["dokkaHtmlPartial"].dependsOn("generateGrammarSource")
}

dependencies {
    fun projectDep(name: String) {
        testImplementation(project(name))
        val mainSourceSet = project(name).dependencyProject.sourceSets.main.get()
        devImplementation(mainSourceSet.clojure.sourceDirectories)
        devImplementation(mainSourceSet.resources.sourceDirectories)
    }

    projectDep(":xtdb-api")
    projectDep(":xtdb-core")

    projectDep(":xtdb-http-server")
    projectDep(":xtdb-http-client-jvm")
    projectDep(":xtdb-pgwire-server")

    projectDep(":modules:xtdb-kafka")
    projectDep(":modules:xtdb-aws")
    projectDep(":modules:xtdb-azure")
    projectDep(":modules:xtdb-google-cloud")

    projectDep(":modules:bench")
    projectDep(":modules:c1-import")
    projectDep(":modules:datasets")
    projectDep(":modules:xtdb-flight-sql")
    projectDep(":cloud-benchmark")

    api("ch.qos.logback", "logback-classic", "1.4.5")

    testImplementation("org.clojure", "data.csv", "1.0.1")
    testImplementation("org.clojure", "tools.logging", "1.2.4")
    testImplementation("org.clojure", "tools.cli", "1.0.206")

    devImplementation("integrant", "repl", "0.3.2")
    devImplementation("com.azure", "azure-identity", "1.9.0")
    devImplementation("com.taoensso", "tufte", "2.6.3")
    devImplementation("clojure.java-time:clojure.java-time:1.4.2")
    testImplementation("org.slf4j", "slf4j-api", "2.0.6")
    testImplementation("com.clojure-goes-fast", "clj-async-profiler", "1.0.0")
    testImplementation("org.postgresql", "postgresql", "42.5.0")
    testImplementation("metosin", "jsonista", "0.3.3")
    testImplementation("clj-commons", "clj-yaml", "1.0.27")
    testImplementation("org.xerial", "sqlite-jdbc", "3.39.3.0")
    testImplementation("org.clojure", "test.check", "1.1.1")
    testImplementation("clj-kondo", "clj-kondo", "2023.12.15")
    testImplementation("com.github.seancorfield", "next.jdbc", "1.3.939")

    // For generating clojure docs
    testImplementation("codox", "codox", "0.10.8")

    // for AWS profiles (managing datasets)
    devImplementation("software.amazon.awssdk", "sts", "2.16.76")
    devImplementation("software.amazon.awssdk", "sso", "2.16.76")
    devImplementation("software.amazon.awssdk", "ssooidc", "2.16.76")

    devImplementation("com.taoensso", "nippy", "3.3.0")
    testImplementation("com.taoensso", "nippy", "3.3.0")

    // hato uses cheshire for application/json encoding
    testImplementation("cheshire", "cheshire", "5.12.0")
}

if (hasProperty("fin")) {
    dependencies {
        devImplementation("vvvvalvalval", "scope-capture", "0.3.3")
    }
}

val codoxOpts = File("${projectDir}/codox.edn").readText()

tasks.create("build-codox", JavaExec::class) {
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass.set("clojure.main")
    jvmArgs(defaultJvmArgs + sixGBJvmArgs)
    val args = mutableListOf(
        "-e", "(require 'codox.main)",
        "-e", "(codox.main/generate-docs ${codoxOpts})"
    )

    this.args = args
}

fun createSltTask(
    taskName: String,
    maxFailures: Long = 0,
    maxErrors: Long = 0,
    testFiles: List<String> = emptyList(),
    extraArgs: List<String> = emptyList(),
) {
    tasks.create(taskName, JavaExec::class) {
        classpath = sourceSets.test.get().runtimeClasspath
        mainClass.set("clojure.main")
        jvmArgs(defaultJvmArgs + sixGBJvmArgs)
        this.args = listOf(
            "-m", "xtdb.sql.logic-test.runner",
            "--verify",
            "--db", "xtdb",
            "--max-failures", maxFailures.toString(),
            "--max-errors", maxErrors.toString(),
        ) + extraArgs + testFiles.map {
            "src/test/resources/xtdb/sql/logic_test/sqlite_test/$it"
        }
    }
}

createSltTask(
    "slt-test",
    testFiles = listOf(
        "xtdb.test",
        "select1.test", "select2.test", "select3.test", "select4.test",
        // "select5.test",
        "random/expr/slt_good_0.test",
        "random/aggregates/slt_good_0.test",
        "random/groupby/slt_good_0.test",
        "random/select/slt_good_0.test"
    )
)

createSltTask(
    "slt-test-2",
    testFiles = listOf(
        "index/between/1/slt_good_0.test",
        "index/commute/10/slt_good_0.test",
        "index/in/10/slt_good_0.test",
        "index/orderby/10/slt_good_0.test",
        "index/orderby_nosort/10/slt_good_0.test",
        "index/random/10/slt_good_0.test",
    )
)

createSltTask(
    "slt-test-dir",
    maxFailures = if (project.hasProperty("testMaxFailures")) {
        val testMaxFailures: String by project
        if (testMaxFailures.isEmpty()) 0 else testMaxFailures.toLong()
    } else 0,

    maxErrors = if (project.hasProperty("testMaxErrors")) {
        val testMaxErrors: String by project
        if (testMaxErrors.isEmpty()) 0 else testMaxErrors.toLong()
    } else 0,

    testFiles = if (project.hasProperty("testDir")) {
        val testDir: String by project
        listOf(testDir)
    } else emptyList(),

    extraArgs = listOf("--dirs")
)

fun createBench(benchName: String, properties: Map<String, String>) {
    tasks.create(benchName, JavaExec::class) {
        classpath = sourceSets.dev.get().runtimeClasspath
        mainClass.set("clojure.main")
        jvmArgs(defaultJvmArgs + sixGBJvmArgs + listOf("-Darrow.enable_unsafe_memory_access=true"))
        val args = mutableListOf("-m", "xtdb.bench.xtdb2", benchName)

        properties.forEach { (k, v) ->
            if (project.hasProperty(v)) {
                args.add(v)
                args.add(project.properties[k] as String)
            }
        }

        if (project.hasProperty("yourkit"))
            jvmArgs("-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so=on_exit=snapshot,async_sampling_cpu,app_name=xtdb-$benchName")

        this.args = args
    }
}

createBench("tpch", mapOf("scaleFactor" to "--scale-factor"))

createBench(
    "auctionmark",
    mapOf(
        "scaleFactor" to "--scale-factor",
        "duration" to "--duration",
        "threads" to "--threads"
    )
)

createBench("products", mapOf("limit" to "--limit"))

tasks.dokkaHtmlMultiModule {
    moduleName.set("")
    moduleVersion.set("2.0.0-SNAPSHOT")

    inputs.file("dokka/logo-styles.css")
    inputs.file("dokka/logo-icon.svg")

    pluginConfiguration<DokkaBase, DokkaBaseConfiguration> {
        customAssets = listOf(file("dokka/logo-icon.svg"))
        customStyleSheets = listOf(file("dokka/logo-styles.css"))

        footerMessage = "© ${Year.now().value} JUXT Ltd"
    }
}
