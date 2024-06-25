package xtdb.api

import kotlinx.serialization.Serializable

@Serializable
data class MetricsConfig(var port: Int = 8080)
