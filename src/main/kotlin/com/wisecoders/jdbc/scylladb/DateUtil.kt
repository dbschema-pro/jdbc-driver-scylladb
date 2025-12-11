package com.wisecoders.jdbc.scylladb

import java.sql.Date
import java.sql.SQLException
import java.sql.Time
import java.sql.Timestamp
import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone
import java.util.function.Function

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
internal object DateUtil {
    private val utcDateFormat = SimpleDateFormat("yyyy-MM-dd")
    private val utcTimeFormat = SimpleDateFormat("HH:mm:ss.SSS")
    private val utcDateTimeFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    private val UTC: TimeZone = TimeZone.getTimeZone("UTC")

    init {
        utcDateFormat.timeZone = UTC
        utcTimeFormat.timeZone = UTC
        utcDateTimeFormat.timeZone = UTC
    }

    private fun getDateFormat(timeZone: TimeZone): SimpleDateFormat {
        if (timeZone == UTC) return utcDateFormat
        val dateFormat = SimpleDateFormat("yyyy-MM-dd")
        dateFormat.timeZone = timeZone
        return dateFormat
    }

    private fun getTimeFormat(timeZone: TimeZone): SimpleDateFormat {
        if (timeZone == UTC) return utcTimeFormat
        val timeFormat = SimpleDateFormat("HH:mm:ss.SSS")
        timeFormat.timeZone = timeZone
        return timeFormat
    }

    private fun getDateTimeFormat(timeZone: TimeZone): SimpleDateFormat {
        if (timeZone == UTC) return utcDateTimeFormat
        val dateTimeFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        dateTimeFormat.timeZone = timeZone
        return dateTimeFormat
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun considerTimeZone(
        timestamp: Timestamp,
        calendar: Calendar,
        direction: Direction
    ): Timestamp {
        val time = considerTimeZone(
            timestamp, calendar.timeZone, direction
        ) { obj: TimeZone -> getDateTimeFormat(obj) }
        val result = Timestamp(time)
        result.nanos = timestamp.nanos
        return result
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun considerTimeZone(
        timestamp: Date,
        calendar: Calendar,
        direction: Direction
    ): Date {
        val time = considerTimeZone(
            timestamp, calendar.timeZone, direction
        ) { obj: TimeZone -> getDateFormat(obj) }
        return Date(time)
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun considerTimeZone(
        timestamp: Time,
        calendar: Calendar,
        direction: Direction
    ): Time {
        val time = considerTimeZone(
            timestamp, calendar.timeZone, direction
        ) { obj: TimeZone -> getTimeFormat(obj) }
        return Time(time)
    }

    @Throws(SQLException::class)
    private fun <T : java.util.Date?> considerTimeZone(
        date: T,
        timeZone: TimeZone,
        direction: Direction,
        formatSupplier: Function<TimeZone, DateFormat>
    ): Long {
        val startValue = direction.formatter(timeZone, formatSupplier).format(date)
        try {
            val resultDate = direction.parser(timeZone, formatSupplier).parse(startValue)
            return resultDate.time
        } catch (e: ParseException) {
            throw SQLException(e)
        }
    }

    internal enum class Direction {
        FROM_UTC {
            override fun formatter(
                timeZone: TimeZone,
                formatSupplier: Function<TimeZone, DateFormat>
            ): DateFormat {
                return formatSupplier.apply(UTC)
            }

            override fun parser(
                timeZone: TimeZone,
                formatSupplier: Function<TimeZone, DateFormat>
            ): DateFormat {
                return formatSupplier.apply(timeZone)
            }
        },
        TO_UTC {
            override fun formatter(
                timeZone: TimeZone,
                formatSupplier: Function<TimeZone, DateFormat>
            ): DateFormat {
                return FROM_UTC.parser(timeZone, formatSupplier)
            }

            override fun parser(
                timeZone: TimeZone,
                formatSupplier: Function<TimeZone, DateFormat>
            ): DateFormat {
                return FROM_UTC.formatter(timeZone, formatSupplier)
            }
        };

        abstract fun formatter(
            timeZone: TimeZone,
            formatSupplier: Function<TimeZone, DateFormat>
        ): DateFormat

        abstract fun parser(
            timeZone: TimeZone,
            formatSupplier: Function<TimeZone, DateFormat>
        ): DateFormat
    }
}
