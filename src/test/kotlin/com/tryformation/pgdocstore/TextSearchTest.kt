package com.tryformation.pgdocstore

import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainAnyOf
import io.kotest.matchers.collections.shouldContainInOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.ints.shouldBeGreaterThan
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

    @Test
    fun shouldRankCorrectly() = coRun {
        val ds = DocStore<SearchableModel>(
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

        ds.documentsByRecency(query = "bar").map { it.title }.let {
            it.first() shouldBe "bar" // clearly the best match
            // we also expect to find these
            it shouldContainAnyOf listOf("ba", "b", "foo bar foobarred")
        }
        ds.documentsByRecency(query = "own", similarityThreshold = 0.01).map { it.title }.let {
            it.first() shouldBe "the quick brown fox" // clearly the best match
        }
        ds.documentsByRecency(query = "own", similarityThreshold = 0.5).map { it.title }.let {
            it shouldHaveSize 0
        }
    }

    @Test
    fun shouldDoTrigrams() = coRun {
        db.sendQuery(
            """
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            DROP INDEX IF EXISTS idx_trigrams_text;
            DROP TABLE IF EXISTS trigrams;
            
            CREATE TABLE IF NOT EXISTS trigrams (
                  id       text PRIMARY KEY,
                  text     text
                );
                
            CREATE INDEX idx_trigrams_text ON trigrams USING gin (text gin_trgm_ops);
                
            INSERT INTO trigrams (id, text) VALUES ('1', 'test@domain.com');
            INSERT INTO trigrams (id, text) VALUES ('2', 'alice@aaa.com');
            INSERT INTO trigrams (id, text) VALUES ('3', 'bob@bobby.com');
            INSERT INTO trigrams (id, text) VALUES ('4', 'the quick brown fox');
            INSERT INTO trigrams (id, text) VALUES ('5', 'the slow yellow fox');
        """.trimIndent()
        )

        val q = "brown fox"
        db.sendQuery(
            """           
            SELECT text, similarity(text, '$q') AS sml FROM trigrams WHERE similarity(text, '$q') > 0.01 ORDER BY sml DESC
        """.trimIndent()
        ).rows.also {
            println(
                """
                RESULTS: ${it.size}
            """.trimIndent()
            )
            it.size shouldBeGreaterThan 0
        }.forEach {
            println("${it["text"]} ${it["sml"]}")
        }
    }
}
