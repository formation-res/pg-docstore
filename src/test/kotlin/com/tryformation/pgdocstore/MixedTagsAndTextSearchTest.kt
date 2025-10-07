package com.tryformation.pgdocstore

import io.kotest.matchers.shouldBe
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days
import org.junit.jupiter.api.Test



class MixedTagsAndTextSearchTest : DbTestBase() {

    @Test
    fun `ensure setting all params works`() = coRun {
        val ds = DocStore(
            dataSource = db,
            serializationStrategy = TaggedModel.serializer(),
            tableName = tableName,
            tagExtractor = TaggedModel::tags,
            idExtractor = TaggedModel::id,
            textExtractor = TaggedModel::title,
        )

        ds.bulkInsert(listOf(
            TaggedModel("one", listOf("foo")),
            TaggedModel("two items", listOf("bar")),
            TaggedModel("three", listOf("foo","bar")),
        ))

        ds.documentsByRecency(listOf("bar"), query = "two items", updatedAtAfter = Clock.System.now()-1.days).size shouldBe 1
    }
}