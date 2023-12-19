package com.jillesvangurp.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.random.Random
import kotlin.random.nextULong


class BulkTest : DbTest() {
    @Test
    fun shouldBulkInsert() = coRun {
        val docFlow = flow {
            repeat(200) {
                emit(UUID.randomUUID().toString() to TestModel(Random.nextULong().toString()))
            }
        }
        val ds = DocStore(db, TestModel.serializer(), tableName)
        ds.bulkInsertWithId(docFlow, chunkSize = 12)
        ds.count() shouldBe 200
        ds.documentsByRecency().count() shouldBe 200
    }
}