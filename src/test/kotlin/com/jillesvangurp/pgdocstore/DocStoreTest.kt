package com.jillesvangurp.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.util.*

@Serializable
data class TestModel(val property: String)

@Serializable
data class TestModelWithId(val property: String, val id: String = UUID.randomUUID().toString())


class DocStoreTest : DbTestBase() {

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
}