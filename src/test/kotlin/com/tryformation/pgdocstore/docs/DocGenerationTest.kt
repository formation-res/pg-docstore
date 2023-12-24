package com.tryformation.pgdocstore.docs

import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.interceptor.LoggingInterceptorSupplier
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import com.jillesvangurp.kotlin4example.SourceRepository
import com.tryformation.pgdocstore.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import java.io.File
import java.util.*
import kotlin.random.Random
import kotlin.random.nextULong

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
                val description: String,
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
            val doc1 = MyModel("Number 1", "a document", categories = listOf("foo"))
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
                repeat(200) {
                    emit(
                        MyModel(
                            title = "Bulk $1",
                            description = "A bulk inserted doc",
                            categories = listOf("bulk")
                        )
                    )
                }
            }.let { f ->
                // bulk insert 40 at a time
                store.bulkInsert(flow = f, chunkSize = 40)
            }


            // and of course we can query
            println( store.documentsByRecency(limit = 5).map { it.title })
            // or we can scroll through the entire table
            // and count the number of documents in the flow
            println("Total documents: ${
                store.documentsByRecencyScrolling().count()
            }")

            // and we can restrict the search using tags
            println("Just the bulk documents: ${
                store
                    .documentsByRecencyScrolling(
                        tags = listOf("bulk")
                    )
                    .count()
            }")
        }


    }

    includeMdFile("outro.md")

    +"""
        This readme is generated with [kotlin4example](https://github.com/jillesvangurp/kotlin4example)
    """.trimIndent()

}