package com.jillesvangurp.pgdocstore

import com.github.jasync.sql.db.SuspendingConnection
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.interceptor.LoggingInterceptorSupplier
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@TestInstance(TestInstance.Lifecycle.PER_CLASS) // needed so we can have @BeforeAll on non static functions
@Execution(ExecutionMode.CONCURRENT)
open class DbTestBase {
    lateinit var tableName : String
    @BeforeAll
    fun beforeAll() = coRun {
        // shared between test models
        tableName = "docs_${Random.nextULong()}"
        db.reCreateDocStoreSchema(tableName)
    }

    @AfterAll
    fun afterAll() = coRun {
        db.dropTable(tableName)
    }

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
): SuspendingConnection {
    return PostgreSQLConnectionBuilder.createConnectionPool {
        this.host = host
        this.port = port
        this.database = db
        this.username = user
        this.password = password

        this.interceptors = mutableListOf(LoggingInterceptorSupplier())
    }.asSuspending
}
