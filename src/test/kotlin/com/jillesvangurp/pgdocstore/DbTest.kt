package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@TestInstance(TestInstance.Lifecycle.PER_CLASS) // needed so we can have @BeforeAll on non static functions
@Execution(ExecutionMode.CONCURRENT)
open class DbTest {

    fun coRun(timeout: Duration = 1.minutes, block: suspend () -> Unit) {
        runBlocking {
            withTimeout(timeout) {
                block.invoke()
            }
        }
    }

    init {
        runBlocking {
            db.reCreateDocStoreSchema()
        }
    }
}

val db by lazy { connect() }
fun connect(
    host: String = "localhost",
    port: Int = 5432,
    db: String = "docstore",
    user: String = "postgres",
    password: String = "secret"
) = PostgreSQLConnectionBuilder.createConnectionPool(
    """jdbc:postgresql://$host:$port/$db?user=$user&password=$password"""
).asSuspending
