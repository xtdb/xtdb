plugins {
    kotlin("jvm")
    id("dev.clojurephant.clojure")
    id("net.corda.plugins.cordapp")
    id("net.corda.plugins.quasar-utils")
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
    implementation("org.clojure", "clojure", "1.10.0")
    implementation("pro.juxt.crux", "crux-core", "1.17.1-rc1")
    implementation("pro.juxt.clojars-mirrors.com.github.seancorfield", "next.jdbc", "1.2.674")
    implementation(project(":crux-corda-state"))

    compileOnly("com.h2database", "h2", "1.4.199")
    compileOnly("org.postgresql", "postgresql", "42.2.17")

    cordaCompile(cordaGroup, "corda-core", cordaVersion)
    cordaCompile(cordaGroup, "corda-jackson", cordaVersion)
    cordaCompile(cordaGroup, "corda-rpc", cordaVersion)
    cordaRuntime(cordaGroup, "corda", cordaVersion)
}
