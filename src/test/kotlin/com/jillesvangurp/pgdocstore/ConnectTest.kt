package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder.createConnectionPool
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes


fun coRun(timeout: Duration = 1.minutes, block: suspend () -> Unit) {
    runBlocking {
        withTimeout(timeout) {
            block.invoke()
        }
    }
}

class ConnectTest {

    @Test
    fun shouldConnect() = coRun {
        val connection = connect()

        connection.sendPreparedStatement(
            """select 1+1 as answer""", listOf()
        ).let {
            it.statusMessage?.let { println(it) }
            it.rows.columnNames().let {
                println(it)
            }
            it.rows.first()["answer"] shouldBe 2
        }
    }
}

fun connect(
    host: String = "localhost",
    port: Int = 5432,
    db: String = "docstore",
    user: String = "postgres",
    password: String = "secret"
) = createConnectionPool(
    """jdbc:postgresql://$host:$port/$db?user=$user&password=$password"""
).asSuspending
