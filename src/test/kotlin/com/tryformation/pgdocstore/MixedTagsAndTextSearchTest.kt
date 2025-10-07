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

        ds.bulkInsert(
            listOf(
                TaggedModel("one", listOf("foo")),
                TaggedModel("two items", listOf("bar")),
                TaggedModel("three", listOf("foo", "bar")),
            )
        )

        ds.documentsByRecency(
            listOf("bar"),
            query = "two items",
            updatedAtAfter = Clock.System.now() - 1.days
        ).size shouldBe 1
    }

    @Test
    fun `ensure normalization works`() = coRun {
        val ds = DocStore(
            dataSource = db,
            serializationStrategy = TaggedModel.serializer(),
            tableName = tableName,
            tagExtractor = TaggedModel::tags,
            idExtractor = TaggedModel::id,
            textExtractor = TaggedModel::title,
            indexNormalizer = { it.reversed() },
            queryNormalizer = { it },
            logging = true
        )

        ds.bulkInsert(
            listOf(
                TaggedModel("one", listOf("foo")),
                TaggedModel("abc", listOf("bar")),
                TaggedModel("three", listOf("foo", "bar")),
            )
        )

        // index normalizer would reverse
        ds.documentsByRecency(
            query = "abc",
            similarityThreshold = 1.0,
            updatedAtAfter = Clock.System.now() - 1.days
        ).size shouldBe 0
        ds.documentsByRecency(
            query = "cba",
            similarityThreshold = 1.0,
            updatedAtAfter = Clock.System.now() - 1.days
        ).size shouldBe 1
    }

    @Test
    fun `switch to like when threshold is 1`() = coRun {
        val ds = DocStore(
            dataSource = db,
            serializationStrategy = TaggedModel.serializer(),
            tableName = tableName,
            tagExtractor = TaggedModel::tags,
            idExtractor = TaggedModel::id,
            textExtractor = TaggedModel::title,
            indexNormalizer = { it.reversed() },
            queryNormalizer = { it },
            logging = true
        )

        ds.bulkInsert(
            listOf(
                TaggedModel("aaa"),
            )
        )

        // index normalizer would reverse
        ds.documentsByRecency(
            query = "aaaa",
            similarityThreshold = 0.9,
            updatedAtAfter = Clock.System.now() - 1.days
        ).size shouldBe 1

        ds.documentsByRecency(
            query = "aaaa",
            // will switch to like
            similarityThreshold = 1.0,
            updatedAtAfter = Clock.System.now() - 1.days
        ).size shouldBe 0

    }
}