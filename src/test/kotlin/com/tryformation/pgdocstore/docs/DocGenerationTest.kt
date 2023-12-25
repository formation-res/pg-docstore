package com.tryformation.pgdocstore.docs

import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import com.jillesvangurp.kotlin4example.SourceRepository
import com.tryformation.pgdocstore.DocStore
import com.tryformation.pgdocstore.db
import com.tryformation.pgdocstore.reCreateDocStoreSchema
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.io.File
import java.util.*
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

        runBlocking {
            connection.reCreateDocStoreSchema("docs")
        }
        @Serializable
        data class MyModel(
            val title: String,
            val description: String? = null,
            val categories: List<String> = listOf(),
            val id: String = UUID.randomUUID().toString(),
        )

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



        subSection("Connecting to the database") {
            +"""
                Pg-docstore uses jasync-postgresql to connect to Postgresql. Unlike many other
                database frameworks, this framework uses non blocking IO and is co-routine friendly;
                and largely written in Kotlin too. This makes it a perfect choice for pg-docstore.
            """.trimIndent()

            suspendingBlock {
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
                connection.reCreateDocStoreSchema("docs")
            }

            +"""
                The `reCreateDocStoreSchema` call applies the docstore table schema to a docs table and re-creates that.
            """.trimIndent()
        }
        subSection("Creating a store") {
            +"""
                We'll use the following simple data class as our model. It's annotated with `@Serializable`. This enables
                model serialization using kotlinx.serialization.
            """.trimIndent()

            suspendingBlock {
                @Serializable
                data class MyModel(
                    val title: String,
                    val description: String? = null,
                    val categories: List<String> = listOf(),
                    val id: String = UUID.randomUUID().toString(),
                )
            }

            +"""
                Using this data class, we can now create a store.
            """.trimIndent()
            suspendingBlock {
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
            }
            +"""
                The call includes three optional arguments. The optional `idExtractor` extracts an id from your model,
                the default implementation simply looks for an `id` property by name but using a property reference is 
                slightly more efficient.
                
                To facilitate querying by plain text (using the text search facilities in postgresql) or tags, 
                there are the textExtractor and tagExtractor fields. This allows you to search across your 
                models without requiring a lot of knowledge about the internal structure of the json. The default 
                implementations simply return `null`. 
                
                In the example, we simply concatenate the title and description to enable text search and we use the 
                content of the categories field to enable tag search.
            """.trimIndent()
        }

        subSection("Create, read, update, and delete (CRUD) and querying") {
            suspendingBlock {

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

                // delete a document
                store.delete(doc1)
                println("now it's gone: ${store.getById(doc1.id)}")
            }
        }
        subSection("Text search") {
            suspendingBlock {

                store.create(MyModel("The quick brown fox"))
                // or search on the extracted text
                println(
                    "Found for 'fox': ${
                        store.documentsByRecency(query = "fox").first().title
                    }"
                )
            }
        }
        subSection("Transactions") {
            +"""
                Most operations in the docstore are single sql statements and don't require a transaction.
                
                But of course we are using a proper database here and you can group operations in a transaction. 
                The `transact` function creates a new docstore with it's own isolated connection that is used for the duration of 
                the transaction. This ensures that it succeeds or rolls back as a whole and prevents other threads 
                from sending sql commands on the same connection. We need this because connections are shared and queries
                are non blocking.
            """.trimIndent()

            suspendingBlock {
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
                    } after the transaction"
                )
            }

            +"""
                In case of an exception, there is a rollback.
            """.trimIndent()

            suspendingBlock {

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

        suspendingBlock {

            // if you like sql, just use the connection
            store.connection
                .sendQuery("SELECT COUNT(*) as total FROM docs")
                .let { res ->
                    res.rows.first().let { row ->
                        println("Count query total: ${row["total"]}")
                    }
                }

        }
    }
    +"""
        This shows off most of the features. And more importantly, it shows how 
        simple interactions with the database are. Mostly, it's just simple idiomatic Kotlin; nice
        little one liners.
    """.trimIndent()

    includeMdFile("outro.md")

    +"""
        This readme is generated with [kotlin4example](https://github.com/jillesvangurp/kotlin4example)
    """.trimIndent()

}