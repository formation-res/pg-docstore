package com.tryformation.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.count
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.util.UUID

@Serializable
data class TaggedModel(val title: String, val tags: List<String>, val id:String = UUID.randomUUID().toString())

class TaggingTest : DbTestBase() {
    @Test
    fun shouldQueryByTag() = coRun {
        val ds = DocStore(
            connection = db,
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

        ds.documentsByRecencyScrolling(listOf("foo")).count() shouldBe 2
        ds.documentsByRecencyScrolling(listOf("foo", "bar")).count() shouldBe 1
        ds.documentsByRecencyScrolling(listOf("foo", "bar"), orTags = true).count() shouldBe 3
    }
}