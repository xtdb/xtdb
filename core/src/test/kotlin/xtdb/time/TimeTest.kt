package xtdb.time

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.TemporalAccessor

class TimeTest {

    private fun ldt(
        year: Int, month: Int = 1, day: Int = 1,
        hour: Int = 0, minute: Int = 0, second: Int = 0, nanoOfSecond: Int = 0
    ) = LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond)

    private val LocalDateTime.z get() = atZone(ZoneId.of("Z"))
    private val LocalDateTime.inLondon get() = atZone(ZoneId.of("Europe/London"))
    private fun LocalDateTime.offset(hours: Int, minutes: Int = 0) = atZone(ZoneOffset.ofHoursMinutes(hours, minutes))

    @Test
    fun `test parsing SQL timestamps`() {
        fun String.check(ta: TemporalAccessor, message: String = "parse '$this'") =
            assertEquals(ta, asSqlTimestamp(), message)

        assertAll(
            "just dates",
            {
                "2020-01-01".check(ldt(2020))
                "2020-02-01".check(ldt(2020, 2))
                "2020-02-02Z".check(ldt(2020, 2, 2).z)
                "2020-02-02+00:00".check(ldt(2020, 2, 2).z)
                "2020-02-02-08:00".check(ldt(2020, 2, 2).offset(-8))

                "2020-02-02Z[Europe/London]".check(ldt(2020, 2, 2).inLondon)
                "2020-08-02+01:00[Europe/London]".check(ldt(2020, 8, 2).inLondon)
            }
        )

        assertAll(
            "local date-times",
            {
                "3000-03-15 20:40:31".check(ldt(3000, 3, 15, 20, 40, 31))
                "3000-03-15 20:40:31.11".check(ldt(3000, 3, 15, 20, 40, 31, 110_000_000))
                "3000-03-15 20:40:31.2222".check(ldt(3000, 3, 15, 20, 40, 31, 222_200_000))
                "3000-03-15 20:40:31.44444444".check(ldt(3000, 3, 15, 20, 40, 31, 444_444_440))
                "3000-03-15T20:40:31".check(ldt(3000, 3, 15, 20, 40, 31))
            }
        )

        assertAll(
            "zoned date-times",
            {
                "3000-03-15 20:40:31+03".check(ldt(3000, 3, 15, 20, 40, 31).offset(3))

                "3000-03-15 20:40:31+03:44".check(ldt(3000, 3, 15, 20, 40, 31).offset(3, 44))
                "3000-03-15 20:40:31.12345678+13:12".check(ldt(3000, 3, 15, 20, 40, 31, 123_456_780).offset(13, 12))
                "3000-03-15 20:40:31.12345678-14:00".check(ldt(3000, 3, 15, 20, 40, 31, 123_456_780).offset(-14))
                "3000-03-15 20:40:31.123456789+14:00".check(ldt(3000, 3, 15, 20, 40, 31, 123_456_789).offset(14))
                "3000-03-15T20:40:31-11:44".check(ldt(3000, 3, 15, 20, 40, 31).offset(-11, -44))

                "3000-03-15T20:40:31Z".check(ldt(3000, 3, 15, 20, 40, 31).z)
                "3000-03-15T20:40:31Z[Europe/London]".check(ldt(3000, 3, 15, 20, 40, 31).inLondon)

                "3000-04-15T20:40:31[Europe/London]".asSqlTimestamp().let {
                    assertEquals(ldt(3000, 4, 15, 20, 40, 31).inLondon, it)

                    assertEquals(
                        ZoneOffset.ofHoursMinutes(1, 0), (it as ZonedDateTime).offset,
                        "provides the offset"
                    )
                }

                "3000-04-15T20:40:31+05:00[Europe/London]".asSqlTimestamp().let {
                    assertEquals(
                        ldt(3000, 4, 15, 16, 40, 31).inLondon, it,
                        "corrects the time (N.B. 16:40)"
                    )

                    assertEquals(
                        ZoneOffset.ofHoursMinutes(1, 0), (it as ZonedDateTime).offset,
                        "corrects the offset"
                    )
                }
            },
        )


        "2024-01-01T00:00Z".check(ldt(2024, 1, 1).z, "missing seconds")
    }
}