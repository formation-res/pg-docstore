package com.tryformation.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.util.*

@Serializable
class SearchableModel(
    val title: String,
    val description: String? = null,
    val id: String = UUID.randomUUID().toString(),
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
}

/**
 *
 * CREATE INDEX pgweb_idx ON pgweb USING GIN (to_tsvector('english', body));
 *
 * UPDATE tt SET ti =
 *     setweight(to_tsvector(coalesce(title,'')), 'A')    ||
 *     setweight(to_tsvector(coalesce(keyword,'')), 'B')  ||
 *     setweight(to_tsvector(coalesce(abstract,'')), 'C') ||
 *     setweight(to_tsvector(coalesce(body,'')), 'D');
 *
 *
 */