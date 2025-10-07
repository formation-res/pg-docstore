package com.tryformation.pgdocstore

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.count
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.time.Duration.Companion.days
import kotlin.time.Clock

class TaggingTest : DbTestBase() {

    @Test
    fun shouldQueryByTag() = coRun {
        val ds = DocStore(
            dataSource = db,
            serializationStrategy = TaggedModel.serializer(),
            tableName = tableName,
            tagExtractor = TaggedModel::tags,
            idExtractor = TaggedModel::id
        )


        ds.bulkInsert(listOf(
            TaggedModel("one", listOf("foo")),
            TaggedModel("two", listOf("bar")),
            TaggedModel("three", listOf("foo","bar")),
        ))

        ds.documentsByRecency(listOf("foo")).count() shouldBe 2
        ds.documentsByRecency(listOf("foo")).count() shouldBe 2
        ds.entriesByRecency(listOf("foo")).count() shouldBe 2
        ds.entriesByRecencyScrolling(listOf("foo")).count() shouldBe 2
        ds.documentsByRecencyScrolling(listOf("foo", "bar")).count() shouldBe 1
        ds.documentsByRecencyScrolling(listOf("foo", "bar"), tagsClauseOperator = BooleanOperator.OR).count() shouldBe 3
    }

    @Test
    fun taggingWithDateRangeCheck() = coRun {
        val ds = DocStore(
            dataSource = db,
            serializationStrategy = TaggedModel.serializer(),
            tableName = tableName,
            tagExtractor = TaggedModel::tags,
            idExtractor = TaggedModel::id
        )

        val now = Clock.System.now()
        ds.create(TaggedModel("one", listOf("xxx")), timestamp = now)
        ds.create(TaggedModel("two", listOf("xxx")), timestamp = now-40.days)
        ds.create(TaggedModel("three", listOf("yyy")), timestamp = now-40.days)
        ds.create(TaggedModel("four", listOf("xxx")), timestamp = now-100.days)

        ds.documentsByRecency(tags=listOf("xxx"), updatedAtAfter = now-50.days, updatedAtBefore = now-30.days).map { it.title } shouldContainExactly  listOf("two")
    }

}