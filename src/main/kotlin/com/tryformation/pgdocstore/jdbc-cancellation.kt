package com.tryformation.pgdocstore

import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private val logger = KotlinLogging.logger { }

internal suspend fun <T> PreparedStatement.query(block: suspend (ResultSet) -> T): T =
    withCancellation { executeQuery().use { block(it) } }

/** Cancel the JDBC statement on a separate virtual thread, and finish cancellation before returning the connection. */
internal suspend fun <T> Statement.withCancellation(block: suspend () -> T): T = coroutineScope {
    val finished = AtomicBoolean(false)
    val cancellation = launch(dbScope, start = CoroutineStart.UNDISPATCHED) {
        try {
            awaitCancellation()
        } finally {
            if (!finished.get()) {
                withContext(NonCancellable) {
                    try {
                        this@withCancellation.cancel()
                    } catch (e: Exception) {
                        logger.debug(e) { "Could not cancel JDBC statement" }
                    }
                }
            }
        }
    }
    try {
        currentCoroutineContext().ensureActive()
        block().also { currentCoroutineContext().ensureActive() }
    } catch (e: Throwable) {
        currentCoroutineContext().ensureActive()
        throw e
    } finally {
        finished.set(true)
        withContext(NonCancellable) { cancellation.cancelAndJoin() }
    }
}

