@file:Suppress("NAME_SHADOWING")

package com.tryformation.pgdocstore.docs

import com.jillesvangurp.kotlin4example.SourceRepository
import com.tryformation.pgdocstore.DocStore
import com.tryformation.pgdocstore.connectionPool
import com.tryformation.pgdocstore.createDocStoreTable
import com.tryformation.pgdocstore.dropDocStoreTable
import java.io.File
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.junit.jupiter.api.Test

private val logger = KotlinLogging.logger { }
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

val tableName = "docs"

@Suppress("UNUSED_VARIABLE")
val readmeMd = sourceGitRepository.md {
    logger.info { "creating connection" }

    val connectionPool = connectionPool(

    )

    logger.info { "creating store" }

    runBlocking {
        connectionPool.dropDocStoreTable(tableName)
        connectionPool.createDocStoreTable(tableName)
    }
    includeMdFile("intro.md")

    logger.info { "usage" }

    section("Usage") {
        @Serializable
        data class MyModel(
            val title: String,
            val description: String? = null,
            val categories: List<String> = listOf(),
            val id: String = UUID.randomUUID().toString(),
        )

        val store = DocStore(
            dataSource = connectionPool(),
            serializationStrategy = MyModel.serializer(),
            tableName = tableName,
            idExtractor = MyModel::id,
            // optional, used for text search
            // FIXME  using lambdas here  somehow hangs the transaction - co-routine weirdness?!
            textExtractor = MyModel::title,
            tagExtractor = MyModel::categories
        )

        subSection("Connecting to the database") {
            +"""
                Pg-docstore uses the jdbc posgresql driver to connect to Postgresql and virtual threads to make that non blocking.                 
            """.trimIndent()

            example(runExample = false) {
                // uses HikariCP but should work with any postgres datasource
                val connectionPool = connectionPool(
                    jdbcUrl = "jdbc:postgresql://localhost:5432/docstore",
                    username = "postgres",
                    password = "secret",
                )
                connectionPool.dropDocStoreTable(tableName)
                connectionPool.createDocStoreTable(tableName)

            }

            +"""
                The `reCreateDocStoreSchema` call applies the docstore table schema to a docs table and re-creates that.
            """.trimIndent()
        }
        logger.info { "store creation" }

        subSection("Creating a store") {
            +"""
                We'll use the following simple data class as our model. It's annotated with `@Serializable`. This enables
                model serialization using kotlinx.serialization.
            """.trimIndent()

            example {
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
            example {
                val store = DocStore(
                    dataSource = connectionPool(),
                    serializationStrategy = MyModel.serializer(),
                    tableName = tableName,
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

        logger.info { "CRUD" }

        subSection("Create, read, update, and delete (CRUD) and querying") {
            runCatching {
                example {

                    // do some crud
                    val doc1 = MyModel("Number 1", "a first document", categories = listOf("foo"))
                    store.create(doc1)

                    store.getById(doc1.id)?.let {
                        println("Retrieved ${it.title}")
                    }

                    // you can only create the same id once
                    try {
                        store.create(doc1)
                    } catch (e: Exception) {
                        // fails
                        println("id already exists")
                    }

                    // you can force the conflict be handled
                    // by overwriting the original
                    store.create(
                        doc1.copy(title = "Numero Uno"),
                        onConflictUpdate = true
                    )

                    // now it is changed
                    store.getById(doc1.id)?.let {
                        println("Changed title ${it.title}")
                    }

                    // This is a better way to do updates
                    // update by id and modify the retrieved original
                    store.update(doc1.id) { original ->
                        // modify the original
                        original.copy(title = "Number One")
                    }

                    // you can also do it like this
                    store.update(doc1) { original ->
                        // still fetches the original
                        original.copy(title = "This is the one")
                    }

                    store.getById(doc1.id)?.let {
                        println("Now it is ${it.title}")
                    }

                    // delete a document
                    store.delete(doc1)
                    println("now it's gone: ${store.getById(doc1.id)}")

                }.let {
                    +"""
                    This prints:
                """.trimIndent()
                    mdCodeBlock(it.stdOut, "text")
                }
            }
        }
        logger.info { "mget" }

        subSection("Multi-get") {
            +"""
                You can get multiple documents in one go like this:
            """.trimIndent()
            example {
                store.create(MyModel(id = "1", title = "Foo"))
                store.create(MyModel(id = "2", title = "Bar"))
                val docs =
                    store.multiGetById(listOf("1", "2"))
                println(docs.map { it.title })
            }.let {
                +"""
                    This prints:
                """.trimIndent()
                mdCodeBlock(it.stdOut, "text")
            }
        }
        subSection("Bulk inserting documents") {
            +"""
                Inserting documents one at the time is not
                very efficient. If you have large amounts
                of documents, you should use bulkInsert.
                
                This works with lists or flows. If you use
                scrolling searches, which return a flow,
                you can efficiently update large amounts
                of documents in one go.
            """.trimIndent()
            example {

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
            }
        }
        logger.info { "querying" }

        subSection("Querying") {
            +"""
                Querying is a bit limited in pg-docstore; it's meant to 
                be used in combination with things like opensearch or
                elasticsearch.
                
                However, there are a few ways in which you can get documents
                out of the store.
            """.trimIndent()

            example {

                // documents are sorted by recency
                println(
                    "five most recent documents: ${
                        store.documentsByRecency(limit = 5).map { it.title }
                    }")
                // we can also scroll through the entire table
                // and count the number of documents in the flow
                println(
                    "Total documents: ${
                        // returns a Flow<T>
                        store.documentsByRecencyScrolling(
                            // fetch pages of 5 rows
                            fetchSize = 5
                        ).count()
                    }"
                )
                // of course you can also do a select count(*) ...
                println(store.count())

                // The search can be restricted using tags
                // you need to set up the tagExtractor functino
                // in the DocStore params for this to work
                println(
                    "Just the bulk tagged documents: ${
                        store.documentsByRecencyScrolling(
                            // filters on the extracted tags
                            tags = listOf("bulk")
                        ).count()
                    }"
                )

            }.let {
                +"""
                    This prints:
                """.trimIndent()
                mdCodeBlock(it.stdOut, "text")
            }

            +"""
                While no substitute for a proper search engine, postgres has some
                text search facilities. We use a simple trigram index which is great
                for common use cases for things like matching and ranking on ids,
                email addresses, parts of words, etc.
            """.trimIndent()
            example {

                store.create(MyModel("The quick brown fox"))
                // or search on the extracted text
                println("Found:")
                store.documentsByRecency(
                    query = "brown fox",
                    // will produce a lot of hits
                    // increase for stricter match
                    similarityThreshold = 0.01,
                    limit = 5
                ).forEach {
                    println("-${it.title}")
                }
            }.let {
                +"""
                    This prints:
                """.trimIndent()
                mdCodeBlock(it.stdOut, "text")
            }
        }
        logger.info { "transactions" }

        subSection("Transactions") {
            +"""
                Most operations in the docstore are single sql statements and don't use a transaction. However,
                you can of course control this if you need to, e.g. modify multiple documents in one transaction.                
                
                The `transact` function creates a new docstore with it's own isolated connection and the same parameters 
                as its parent. The isolated connection is used for the duration of 
                the transaction and exclusive to your code block. It will commit / rollback as needed. 
                                 
            """.trimIndent()

            example {
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
            }.let {
                +"""
                    This prints:
                """.trimIndent()
                mdCodeBlock(it.stdOut, "text")
            }

            +"""
                In case of an exception, there is a rollback.
            """.trimIndent()

            example {
                // rollbacks happen if there are exceptions
                val another = MyModel(
                    title = "Transactional",
                    description = "yet another document",
                    categories = listOf("foo")
                )
                runCatching {
                    store.transact { tStore ->
                        tStore.create(another)

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
            }.let {
                +"""
                    This prints:
                """.trimIndent()
                mdCodeBlock(it.stdOut, "text")
            }
        }
    }

    includeMdFile("outro.md")

    +"""
        This readme is generated with [kotlin4example](https://github.com/jillesvangurp/kotlin4example)
    """.trimIndent()

    logger.info { "done" }
}