package com.jillesvangurp.pgdocstore

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.Test

class TransactTest : DbTestBase() {

    @Test
    fun shouldRollback() = coRun {
        val ds = DocStore(db, TestModelWithId.serializer(), tableName)
        val doc = TestModelWithId("bla")
        shouldThrow<Exception> {
            ds.transact { dsT ->
                dsT.create(doc)
                // document exists
                dsT.getById(doc.id) shouldNotBe null
                val doc2 = TestModelWithId("foobar", id = "idontexist")
                dsT.update(doc2) {
                    it.copy(property = "fail")
                }
            }
        }
        // rollback should have removed the document
        ds.getById(doc.id) shouldBe null
    }

    @Test
    fun nestedTransactShouldBeFine() = coRun {
        val ds1 = DocStore(db, TestModelWithId.serializer(), tableName)
        val doc = TestModelWithId("bla")
        ds1.transact { ds2 ->
            ds2.transact { ds3 ->
                ds3.create(doc)
            }
        }
        ds1.getById(doc.id) shouldNotBe null
    }
}