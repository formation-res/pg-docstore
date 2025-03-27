package com.tryformation.pgdocstore

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@TestInstance(TestInstance.Lifecycle.PER_CLASS) // needed so we can have @BeforeAll on non static functions
// we are doing things concurrently on purpose because it is faster and it will become flaky unless we do things properly with
// connections and transactions. So, if the tests are  flaky, we have serious bugs. It should be fast and not flaky.
// beware: class level state between test functions should not be modified by the tests. Therefore: use BeforeAll instead
// of BeforeEach.
@Execution(ExecutionMode.CONCURRENT)
open class DbTestBase {
    lateinit var tableName : String

    @BeforeAll
    fun beforeAll() = coRun {
        // all tests in the same class have the same table
        tableName = "docs_${Random.nextULong()}"

        db.createDocStoreTable(tableName)
    }

    @AfterAll
    fun afterAll() = coRun {
        db.dropDocStoreTable(tableName)
    }

    fun coRun(timeout: Duration = 1.minutes, block: suspend () -> Unit) {
        runBlocking {
            withTimeout(timeout) {
                block.invoke()
            }
        }
    }
}

val db by lazy {
    connectionPool()
}

