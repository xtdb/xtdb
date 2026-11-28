package xtdb.operator.scan

import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
internal val IID_ORDERING_BOUNDARIES: List<Uuid> = listOf(
    "00000000-0000-0000-0000-000000000000",
    "00000000-0000-0000-0000-000000000001",
    "00000000-0000-0000-7fff-ffffffffffff",
    "00000000-0000-0000-8000-000000000000",
    "7fffffff-ffff-ffff-ffff-ffffffffffff",
    "80000000-0000-0000-0000-000000000000",
    "80000000-0000-0000-8000-000000000000",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
).map(Uuid::parse)
