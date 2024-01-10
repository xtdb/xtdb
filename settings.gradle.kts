pluginManagement {
    plugins {
        kotlin("jvm") version "1.9.22"
        kotlin("plugin.serialization") version "1.9.22"
    }
}

rootProject.name = "xtdb"

include("api", "core")
include("http-server", "http-client-clj", "pgwire-server")
include("docker:standalone","docker:aws")

include("modules:jdbc", "modules:kafka", "modules:s3", "modules:azure", "modules:google-cloud")
include("modules:c1-import", "modules:flight-sql")
include("modules:bench", "modules:datasets")
