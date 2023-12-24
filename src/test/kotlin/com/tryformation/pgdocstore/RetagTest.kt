package com.tryformation.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import org.junit.jupiter.api.Test

class RetagTest: DbTestBase() {
    @Test
    fun `should retag everything with updated tag extractor`() = coRun {
        val ds = DocStore(db, TaggedModel.serializer(), tableName)

        flow {
            repeat(200) {
                emit(TaggedModel(title = "doc $it", tags = listOf("foo")))
            }
        }.let {
            ds.bulkInsert(it)
        }
        ds.documentsByRecencyScrolling(tags = listOf("foo")).count()  shouldBe 0
        val ds2 = DocStore(db, TaggedModel.serializer(), tableName, tagExtractor = TaggedModel::tags)
        ds2.reExtract()
        ds2.documentsByRecencyScrolling(tags = listOf("foo")).count()  shouldBe 200
    }
}