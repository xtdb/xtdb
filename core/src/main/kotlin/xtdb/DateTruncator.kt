package xtdb

import java.time.DayOfWeek
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime.MIDNIGHT
import java.time.ZonedDateTime
import java.time.LocalDate.of as date
import java.time.LocalDateTime.of as dateTime

object DateTruncator {

    @JvmStatic
    fun truncateYear(date: LocalDate, yearMultiple: Int) =
        date((date.year / yearMultiple) * yearMultiple, 1, 1)

    @JvmStatic
    fun truncateYear(dateTime: LocalDateTime, yearMultiple: Int) =
        dateTime(truncateYear(dateTime.toLocalDate(), yearMultiple), MIDNIGHT)

    @JvmStatic
    fun truncateYear(zdt: ZonedDateTime, yearMultiple: Int) =
        zdt.with(truncateYear(zdt.toLocalDateTime(), yearMultiple))

    @JvmStatic
    fun truncateYear(date: LocalDate) = date(date.year, 1, 1)

    @JvmStatic
    fun truncateYear(dateTime: LocalDateTime) = dateTime(truncateYear(dateTime.toLocalDate()), MIDNIGHT)

    @JvmStatic
    fun truncateYear(zdt: ZonedDateTime) = zdt.with(truncateYear(zdt.toLocalDateTime()))

    @JvmStatic
    fun truncateQuarter(date: LocalDate) = date(date.year, date.month.firstMonthOfQuarter(), 1)

    @JvmStatic
    fun truncateQuarter(dateTime: LocalDateTime) = dateTime(truncateQuarter(dateTime.toLocalDate()), MIDNIGHT)

    @JvmStatic
    fun truncateQuarter(zdt: ZonedDateTime) = zdt.with(truncateQuarter(zdt.toLocalDateTime()))

    @JvmStatic
    fun truncateMonth(date: LocalDate) = date(date.year, date.month, 1)

    @JvmStatic
    fun truncateMonth(dateTime: LocalDateTime) = dateTime(truncateMonth(dateTime.toLocalDate()), MIDNIGHT)

    @JvmStatic
    fun truncateMonth(zdt: ZonedDateTime) = zdt.with(truncateMonth(zdt.toLocalDateTime()))

    @JvmStatic
    fun truncateWeek(date: LocalDate) = date.with(DayOfWeek.MONDAY)

    @JvmStatic
    fun truncateWeek(dateTime: LocalDateTime) = dateTime(truncateWeek(dateTime.toLocalDate()), MIDNIGHT)

    @JvmStatic
    fun truncateWeek(zdt: ZonedDateTime) = zdt.with(truncateWeek(zdt.toLocalDateTime()))
}