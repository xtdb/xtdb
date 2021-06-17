import dev.clojurephant.plugin.clojure.tasks.ClojureNRepl

plugins {
    kotlin("jvm")
    id("net.corda.plugins.cordapp")
    id("net.corda.plugins.quasar-utils")
    id("dev.clojurephant.clojure")
}

val cordaGroup = "net.corda"
val cordaVersion = "4.5"

cordapp {
    workflow {
        targetPlatformVersion = 4
        minimumPlatformVersion = 4
    }
}

dependencies {
    cordaCompile(cordaGroup, "corda-core", cordaVersion)
    cordaCompile(cordaGroup, "corda-jackson", cordaVersion)
    cordaCompile(cordaGroup, "corda-rpc", cordaVersion)
    cordaRuntime(cordaGroup, "corda", cordaVersion)
    cordapp(project(":iou-contract"))
    cordapp(project(":crux-corda-state"))
    cordapp(project(":crux-corda"))
    implementation("juxt", "crux-core", "21.06-1.17.1-beta")

    testImplementation("junit", "junit", "4.12")
    testImplementation(cordaGroup, "corda-node-driver", cordaVersion)
    testImplementation("org.clojure", "clojure", "1.10.0")

    testImplementation("com.h2database", "h2", "1.4.199")
    testImplementation("org.postgresql", "postgresql", "42.2.17")
}

tasks.withType(Test::class) {
    enableAssertions = false
}

tasks.withType(ClojureNRepl::class.java) {
    forkOptions {
        jvmArgs!!.add("-javaagent:${project.configurations["quasar"].singleFile}")
    }
}

quasar {
    excludePackages.addAll(file("$projectDir/src/main/resources/quasar-exclude.txt").readLines())
}
