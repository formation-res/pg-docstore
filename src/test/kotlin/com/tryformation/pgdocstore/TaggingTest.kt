package com.tryformation.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.count
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.util.UUID

@Serializable
data class TaggedModel(val title: String, val tags: List<String> = listOf(), val id:String = UUID.randomUUID().toString())

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

}