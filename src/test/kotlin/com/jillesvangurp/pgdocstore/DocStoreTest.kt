package com.jillesvangurp.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test

@Serializable
data class TestModel(val property: String)

class DocStoreTest : DbTest() {
    @Test
    fun shouldDoCrud() = coRun{

        val ds = DocStore(db, TestModel.serializer())

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
}