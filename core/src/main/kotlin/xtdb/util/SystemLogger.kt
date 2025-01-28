package xtdb.util

import java.lang.System.Logger
import java.lang.System.Logger.Level.*
import kotlin.reflect.KClass

val KClass<*>.logger: Logger get() = System.getLogger(qualifiedName)

fun Logger.trace(message: String) = log(TRACE, message)
fun Logger.trace(throwable: Throwable, message: String) = log(TRACE, message, throwable)

fun Logger.debug(message: String) = log(DEBUG, message)
fun Logger.debug(throwable: Throwable, message: String) = log(DEBUG, message, throwable)

fun Logger.info(message: String) = log(INFO, message)
fun Logger.info(throwable: Throwable, message: String) = log(INFO, message, throwable)

fun Logger.warn(message: String) = log(WARNING, message)
fun Logger.warn(throwable: Throwable, message: String) = log(WARNING, message, throwable)

fun Logger.error(message: String) = log(ERROR, message)
fun Logger.error(throwable: Throwable, message: String) = log(ERROR, message, throwable)
