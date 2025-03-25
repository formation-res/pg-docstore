package com.tryformation.pgdocstore

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds


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
    fun deleteOfIdThatDoesNotExist() = coRun{
        val ds = DocStore(db, TestModel.serializer(), tableName)
        ds.delete("idontexist")
    }

    @Test
    fun updateOfIdThatDoesnNotExist() = coRun {
        val ds = DocStore(db, TestModel.serializer(), tableName)
        shouldThrow<DocumentNotFoundException> {
            ds.update("idontexist") {
                it
            }
        }

    }

    @Test
    fun shouldUseIdExtractor() = coRun {
        val ds = DocStore(db, TestModelWithId.serializer(), tableName)
        val doc = TestModelWithId("foo")
        ds.create(doc)
        ds.getById(doc.id) shouldBe doc
        ds.update(doc) {
            it.copy(title = "bar")
        }
        ds.getById(doc.id)?.title shouldBe "bar"
        ds.delete(doc)
        ds.getById(doc.id) shouldBe null
    }

    @Test
    fun `update should change timestamp`() = coRun {
        val ds = DocStore(db, TestModelWithId.serializer(), tableName)
        val doc = TestModelWithId("foo")
        ds.create(doc)
        delay(1.seconds)
        val ts1 = ds.getEntryById(doc.id)!!.updatedAt
        ds.update(doc) {
            it.copy(title = "bar")
        }
        ds.getEntryById(doc.id)!!.updatedAt shouldNotBe ts1
    }

    @Test
    fun `should handle conflicting writes`() = coRun {
        val ds = DocStore(db, TestModelWithId.serializer(), tableName)
        val doc = TestModelWithId("Foo")
        ds.create(doc)
        ds.getEntryById(doc.id)!!.let {
            it.createdAt shouldBe  it.updatedAt
            it.document<TestModelWithId>().title shouldBe "Foo"
        }

        assertThrows<Exception> {
            // double create is not allowed
            ds.create(doc)
        }
        delay(200.milliseconds)
        ds.create(doc.copy(title = "Bar"), onConflictUpdate = true)
        ds.getEntryById(doc.id)!!.let {
            it.createdAt shouldNotBe it.updatedAt
            it.document<TestModelWithId>().title shouldBe "Bar"
        }
    }
}