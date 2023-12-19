package com.jillesvangurp.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.random.Random
import kotlin.random.nextULong


class BulkTest : DbTestBase() {
    @Test
    fun shouldBulkInsert() = coRun {
        val docFlow = flow {
            repeat(200) {
                emit(TestModelWithId(Random.nextULong().toString()))
            }
        }
        val ds = DocStore(db, TestModelWithId.serializer(), tableName)
        ds.bulkInsert(docFlow, chunkSize = 12)
        ds.count() shouldBe 200
        val stored = ds.documentsByRecency().toList()
        stored.size shouldBe 200
        stored.map { it.copy(property = it.property.reversed()) }.let {
            ds.bulkInsert(it)
        }
        ds.count() shouldBe 200
    }
}