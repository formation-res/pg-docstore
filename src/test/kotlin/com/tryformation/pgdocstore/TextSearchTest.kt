package com.tryformation.pgdocstore

import io.kotest.matchers.collections.shouldContainAnyOf
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.util.*

@Serializable
class SearchableModel(
    val title: String,
    val description: String? = null,
    val id: String = UUID.randomUUID().toString(),
    val tags: List<String> = emptyList()
)

class TextSearchTest : DbTestBase() {

    @Test
    fun shouldSearchOnTitleBulkInsert() = coRun {
        val ds = DocStore<SearchableModel>(
            db,
            SearchableModel.serializer(),
            tableName,
            textExtractor = { listOfNotNull(it.title, it.description).joinToString("\n") })
        ds.bulkInsert(
            listOf(
                SearchableModel("The quick brown fox", "Lorum ipsum"),
                SearchableModel("foo bar", "bar"),
            )
        )
        ds.documentsByRecency(query = "ipsum").count() shouldBe 1
    }

    @Test
    fun shouldSearchOnTitleSingleInsert() = coRun {
        val ds = DocStore<SearchableModel>(
            db,
            SearchableModel.serializer(),
            tableName,
            textExtractor = { listOfNotNull(it.title, it.description).joinToString("\n") })
        ds.create(
            SearchableModel("To be or not to be", "stopword challenge")
        )
        ds.create(
            SearchableModel("Hasta La Vista", "The governator"),
        )
        ds.documentsByRecency(query = "stopword").count() shouldBe 1
    }

    @Test
    fun shouldRankCorrectly() = coRun {
        val ds = DocStore(
            db,
            SearchableModel.serializer(),
            tableName,
            textExtractor = { listOfNotNull(it.title, it.description).joinToString("\n") })

        val docs = listOf(
            "the quick brown fox",
            "the lazy green turtle",
            "the sluggish pink elephant",
            "foo bar foobarred",
            "bar",
            "ba",
            "b",
            "foo bar",
        ).map { SearchableModel(it) }
        ds.bulkInsert(docs)

        ds.search(query = "bar").map { it.title }.let {
            it.first() shouldBe "bar" // clearly the best match
            // we also expect to find these
            it shouldContainAnyOf listOf("ba", "b", "foo bar foobarred")
        }
        ds.search(query = "own", similarityThreshold = 0.01).map { it.title }.let {
            it.first() shouldBe "the quick brown fox" // clearly the best match
        }
        ds.search(query = "own", similarityThreshold = 0.5).map { it.title }.let {
            it shouldHaveSize 0
        }
    }

    @Test
    fun shouldDoANDorOR() = coRun {
        val ds = DocStore(
            db,
            SearchableModel.serializer(),
            tableName,
            textExtractor = { listOfNotNull(it.title, it.description).joinToString("\n") },
            tagExtractor = SearchableModel::tags
        )
        ds.bulkInsert(
            listOf(
                SearchableModel("Document Numero Uno", tags = listOf("foo", "bar")),
                SearchableModel("The second one", tags = listOf("foo")),
                SearchableModel("Another Document", tags = listOf("bar")),
            )
        )
        ds.documentsByRecency(
            tags = listOf("foo"),
            tagsClauseOperator = BooleanOperator.AND,
            query = "Another",
            whereClauseOperator = BooleanOperator.AND,
        ) shouldHaveSize 0
        ds.documentsByRecency(
            tags = listOf("foo"),
            tagsClauseOperator = BooleanOperator.AND,
            query = "Another",
            whereClauseOperator = BooleanOperator.OR,
        ) shouldHaveSize 3
        ds.documentsByRecency(
            tags = listOf("foo", "foobar"),
            tagsClauseOperator = BooleanOperator.AND,
            query = "Another",
            whereClauseOperator = BooleanOperator.AND,
        ) shouldHaveSize 0
        ds.documentsByRecency(
            tags = listOf("foo", "foobar"),
            tagsClauseOperator = BooleanOperator.OR,
            query = "Document",
            whereClauseOperator = BooleanOperator.AND,
        ) shouldHaveSize 1

    }
}

private suspend fun DocStore<*>.search(query: String, similarityThreshold: Double = 0.1) =
    entriesByRecency(query = query, similarityThreshold = similarityThreshold).also {
        println("Found for '$query' with threshold $similarityThreshold:")
        it.forEach { e ->
            val d = e.document<SearchableModel>()
            println("- ${d.title} (${e.similarity})")
        }
    }.map {
        it.document<SearchableModel>()
    }