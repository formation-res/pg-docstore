package com.tryformation.pgdocstore

import javax.sql.DataSource
import kotlin.test.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class MultiStoreTransactTest : DbTestBase() {
    private val otherTable get() = "${tableName}_memberships"
    private val users get() = DocStore(db, TestModelWithId.serializer(), tableName)
    private val memberships get() = DocStore(
        db, TaggedModel.serializer(), otherTable,
        tagExtractor = TaggedModel::tags, textExtractor = TaggedModel::title,
    )

    @BeforeAll
    fun createOtherTable() = coRun { db.createDocStoreTable(otherTable) }

    @AfterAll
    fun dropOtherTable() = coRun { db.dropDocStoreTable(otherTable) }

    @Test
    fun `different document types commit together and return a result`() = coRun {
        val user = TestModelWithId("before")
        val membership = TaggedModel("before", listOf("member"))
        val result = db.transact {
            val u = bind(users)
            val m = bind(memberships)
            u.create(user)
            m.create(membership)
            u.update(user.id) { it.copy(title = "after") }
            m.update(membership.id) { it.copy(title = "after") }
            assertEquals("after", u.getById(user.id)?.title)
            assertEquals("after", m.getById(membership.id)?.title)
            // Ordinary stores do not accidentally join the transaction.
            assertNull(users.getById(user.id))
            assertNull(memberships.getById(membership.id))
            "committed"
        }
        assertEquals("committed", result)
        assertEquals("after", users.getById(user.id)?.title)
        assertEquals("after", memberships.getById(membership.id)?.title)
    }

    @Test
    fun `failure rolls back changes in both tables including nested transactions`() = coRun {
        val user = TestModelWithId("before")
        val membership = TaggedModel("before")
        users.create(user)
        memberships.create(membership)
        assertFailsWith<IllegalStateException> {
            db.transact {
                val u = bind(users)
                val m = bind(memberships)
                u.transact { nested ->
                    assertSame(u, nested)
                    nested.update(user.id) { it.copy(title = "after") }
                    m.transact { it.update(membership.id) { it.copy(title = "after") } }
                }
                error("roll back both")
            }
        }
        assertEquals("before", users.getById(user.id)?.title)
        assertEquals("before", memberships.getById(membership.id)?.title)
    }

    @Test
    fun `cancellation rolls back both stores and invalidates bindings`() = coRun {
        val user = TestModelWithId("cancelled")
        val membership = TaggedModel("cancelled")
        val written = CompletableDeferred<IDocStore<TestModelWithId>>()
        coroutineScope {
            val job = launch {
                db.transact {
                    val u = bind(users)
                    u.create(user)
                    bind(memberships).create(membership)
                    written.complete(u)
                    awaitCancellation()
                }
            }
            val escaped = written.await()
            job.cancelAndJoin()
            assertFailsWith<IllegalStateException> { escaped.getById(user.id) }
        }
        assertNull(users.getById(user.id))
        assertNull(memberships.getById(membership.id))
    }

    @Test
    fun `bindings and deferred flows expire on success and failure`() = coRun {
        for (fail in listOf(false, true)) {
            lateinit var escaped: IDocStore<TestModelWithId>
            lateinit var scope: DocStoreTransaction
            lateinit var deferred: Flow<TestModelWithId>
            try {
                db.transact {
                    scope = this
                    escaped = bind(users)
                    deferred = escaped.documentsByRecencyScrolling()
                    if (fail) error("rollback")
                }
            } catch (_: IllegalStateException) {
                assertTrue(fail)
            }
            assertFailsWith<IllegalStateException> { scope.bind(users) }
            assertFailsWith<IllegalStateException> { escaped.count() }
            assertFailsWith<IllegalStateException> { escaped.transact { } }
            assertFailsWith<IllegalStateException> { escaped.deleteByIds(emptyList()) }
            assertFailsWith<IllegalStateException> { escaped.multiGetById(emptyList()) }
            assertFailsWith<IllegalStateException> { escaped.bulkInsert(emptyList()) }
            assertFailsWith<IllegalStateException> { deferred.toList() }
        }
    }

    @Test
    fun `flows can be collected sequentially inside the transaction`() = coRun {
        val user = TestModelWithId("cursor")
        val membership = TaggedModel("cursor", listOf(user.id))
        db.transact {
            val u = bind(users)
            val m = bind(memberships)
            u.create(user)
            m.create(membership)
            assertTrue(u.documentsByRecencyScrolling(fetchSize = 1).toList().any { it.id == user.id })
            assertEquals(listOf(membership), m.documentsByRecencyScrolling(tags = listOf(user.id)).toList())
        }
    }

    @Test
    fun `rejects another data source and stores already bound elsewhere`() = coRun {
        val otherSource = object : DataSource by db {}
        val otherStore = DocStore(otherSource, TestModelWithId.serializer(), tableName)
        db.transact {
            assertFailsWith<IllegalArgumentException> { bind(otherStore) }
            val bound = bind(users) as DocStore<TestModelWithId>
            assertSame(bound, bind(bound))
            db.transact {
                assertFailsWith<IllegalArgumentException> { bind(bound) }
            }
        }
        db.withConnection { connection ->
            val boundToConnection = DocStore(db, TestModelWithId.serializer(), tableName, connection = connection)
            db.transact {
                assertFailsWith<IllegalArgumentException> { bind(boundToConnection) }
            }
        }
    }

    @Test
    fun `database error in second store rolls back first store`() = coRun {
        val user = TestModelWithId("rolled back")
        val membership = TaggedModel("existing")
        memberships.create(membership)
        assertFailsWith<java.sql.SQLException> {
            db.transact {
                bind(users).create(user)
                bind(memberships).create(membership) // Duplicate primary key.
            }
        }
        assertNull(users.getById(user.id))
        assertEquals(membership, memberships.getById(membership.id))
    }

    @Test
    fun `single store transaction bindings also expire`() = coRun {
        val escaped = users.transact { it }
        assertFailsWith<IllegalStateException> { escaped.count() }
    }

    @Test
    fun `separate data source transaction is independent`() = coRun {
        val user = TestModelWithId("rolled back")
        val membership = TaggedModel("independent")
        assertFailsWith<IllegalStateException> {
            db.transact {
                bind(users).create(user)
                db.transact { bind(memberships).create(membership) }
                error("rollback outer")
            }
        }
        assertNull(users.getById(user.id))
        assertEquals(membership, memberships.getById(membership.id))
    }
}
