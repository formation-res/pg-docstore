package com.jillesvangurp.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.random.Random
import kotlin.random.nextULong

@Serializable
data class TestModel(val property: String)

@Serializable
data class TestModelWithId(val property: String, val id: String = UUID.randomUUID().toString())

private val tableName = "docs_${Random.nextULong()}"

class DocStoreTest : DbTest() {
    @BeforeAll
    fun beforeAll() = coRun {
        db.reCreateDocStoreSchema(tableName)
    }

    @AfterAll
    fun afterAll() = coRun {
        db.dropTable(tableName)
    }

    @Test
    fun shouldDoCrud() = coRun {

        val ds = DocStore(db, TestModel.serializer(), tableName)

        runCatching {
            // clean up from previous test
            ds.delete("42")
        }
        ds.create("42", TestModel("bar"))
        ds.getById("42")?.property shouldBe "bar"
        ds.update("42") {
            it.copy(property = "foo")
        }
        ds.getById("42")?.property shouldBe "foo"
        ds.delete("42")
        ds.getById("42") shouldBe null
    }

    @Test
    fun shouldUseIdExtractor() = coRun {
        val ds = DocStore(db, TestModelWithId.serializer(), tableName)
        val doc = TestModelWithId("foo")
        ds.create(doc)
        ds.getById(doc.id) shouldBe doc
        ds.update(doc) {
            it.copy(property = "bar")
        }
        ds.getById(doc.id)?.property shouldBe "bar"
        ds.delete(doc)
        ds.getById(doc.id) shouldBe null

    }

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
        ds.queryFlow("SELECT * FROM $tableName ORDER BY updated_at DESC", listOf()).toList().size shouldBe 200
    }
}