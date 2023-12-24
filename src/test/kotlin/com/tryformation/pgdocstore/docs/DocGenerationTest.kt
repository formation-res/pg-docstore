package com.tryformation.pgdocstore.docs

import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.interceptor.LoggingInterceptorSupplier
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import com.jillesvangurp.kotlin4example.SourceRepository
import com.tryformation.pgdocstore.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.io.File
import java.util.*
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.time.Duration.Companion.milliseconds

const val githubLink = "https://github.com/formation-res/pg-docstore"

val sourceGitRepository = SourceRepository(
    repoUrl = githubLink,
    sourcePaths = setOf("src/main/kotlin", "src/test/kotlin")
)

class DocGenerationTest {

    @Test
    fun `generate docs`() {
        File(".", "README.md").writeText(
            """
            # Pg Doc Store 
            
        """.trimIndent().trimMargin() + "\n\n" + readmeMd.value
        )
    }
}

val readmeMd = sourceGitRepository.md {
    includeMdFile("intro.md")

    section("Usage") {

        suspendingBlock {
            // jasync suspending connection
            val connection = PostgreSQLConnectionBuilder
                .createConnectionPool(
                    ConnectionPoolConfiguration(
                        host = "localhost",
                        port = 5432,
                        database = "docstore",
                        username = "postgres",
                        password = "secret",
                    )
                ).asSuspending

            // recreate the docs table
            db.reCreateDocStoreSchema("docs")

            @Serializable
            data class MyModel(
                val title: String,
                val description: String? = null,
                val categories: List<String> = listOf(),
                val id: String = UUID.randomUUID().toString(),
            )

            // create a store for the docs table
            val store = DocStore(
                connection = connection,
                serializationStrategy = MyModel.serializer(),
                tableName = "docs",
                idExtractor = MyModel::id,
                // optional, used for text search
                textExtractor = {
                    "${it.title} ${it.description}"
                },
                // optional, used for tag search
                tagExtractor = {
                    it.categories
                }
            )

            // do some crud
            val doc1 = MyModel("Number 1", "a first document", categories = listOf("foo"))
            store.create(doc1)
            store.getById(doc1.id)?.let {
                println("Retrieved ${it.title}")
            }
            // update by id
            store.update(doc1.id) {
                it.copy(title = "Number One")
            }
            // or just pass in the document
            store.update(doc1) {
                it.copy(title = "Numero Uno")
            }
            // Retrieve it again
            store.getById(doc1.id)?.let {
                println("Retrieved ${it.title}")
            }

            // you can also do bulk inserts using flows or lists
            flow {
                repeat(200) { index ->
                    emit(
                        MyModel(
                            title = "Bulk $index",
                            description = "A bulk inserted doc #$index",
                            categories = listOf("bulk")
                        )
                    )
                    // ensure the documents don't have the same timestamp
                    delay(1.milliseconds)
                }
            }.let { f ->
                // bulk insert 40 at a time
                store.bulkInsert(flow = f, chunkSize = 40)
            }

            // and of course we can query in all sorts of ways
            println("five most recent documents: ${
                store.documentsByRecency(limit = 5).map { it.title }
            }")
            // we can scroll through the entire table
            // and count the number of documents in the flow
            println(
                "Total documents: ${
                    store.documentsByRecencyScrolling().count()
                }"
            )

            // and we can restrict the search using tags
            println(
                "Just the bulk tagged documents: ${
                    store
                        .documentsByRecencyScrolling(
                            // filters on the extracted tags
                            tags = listOf("bulk")
                        )
                        .count()
                }"
            )

            // or search on the extracted tag
            println(
                "Found for 'first': ${
                    store.documentsByRecency(query = "first").first().title
                }"
            )

            // if you like sql, just use the connection
            store.connection
                .sendQuery("SELECT COUNT(*) as total FROM docs")
                .let { res ->
                    res.rows.first().let { row ->
                        println("Count query total: ${row["total"]}")
                    }
                }

            // the whole point of dbs is transactions
            store.transact { tStore ->
                // everything you do with tStore is
                // one big transaction
                tStore.create(
                    MyModel(
                        title = "One more",
                        categories = listOf("transaction1")
                    )
                )
                tStore.create(
                    MyModel(
                        title = "And this one too",
                        categories = listOf("transaction1")
                    )
                )
            }
            println(
                "Both docs exist: ${
                    store.documentsByRecency(tags = listOf("transaction1")).count()
                }"
            )

            // rollbacks happen if there are exceptions
            val another = MyModel(
                title = "Another",
                description = "yet another document",
                categories = listOf("foo")
            )
            runCatching {
                store.transact { tStore ->
                    tStore.create(another)
                    tStore.update(another) {
                        another.copy(title = "Modified")
                    }
                    println(
                        "in the transaction it exists as: ${
                            tStore.getById(another.id)?.title
                        }"
                    )
                    // simulate an error
                    // causes a rollback and rethrows
                    error("oopsie")
                }
            }
            // prints null because the transaction was rolled back
            println("after the rollback ${store.getById(another.id)?.title}")
        }
    }

    includeMdFile("outro.md")

    +"""
        This readme is generated with [kotlin4example](https://github.com/jillesvangurp/kotlin4example)
    """.trimIndent()

}