package com.tryformation.pgdocstore

import java.sql.Connection
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/**
 * Binds stores to one transaction. Use bound stores sequentially and collect their flows
 * before the block returns. Bindings cannot be used after the transaction block ends.
 */
class DocStoreTransaction internal constructor(
    internal val dataSource: DataSource,
    internal val connection: Connection,
) {
    private val active = AtomicBoolean(true)

    /**
     * Returns a store using this transaction's connection. The store must use the exact
     * same [DataSource] instance and must not be bound to a different transaction.
     * Only JDBC-backed [DocStore] instances participate; [IDocStore] is unchanged.
     */
    fun <T : Any> bind(store: DocStore<T>): IDocStore<T> = store.bindTo(this)

    internal fun checkActive() {
        check(active.get()) { "Docstore transaction scope has ended" }
    }

    internal fun close() {
        active.set(false)
    }
}

/**
 * Runs operations on multiple bound stores in one PostgreSQL transaction.
 * Returns the block result, commits on success, and rolls back on failure or cancellation.
 *
 * Use [DocStoreTransaction.bind] for every participating store. Original, unbound stores
 * continue to acquire their own connections. Bound stores and flows must stay inside the
 * block and be used sequentially, including while a cursor flow is being collected.
 *
 * Calling [IDocStore.transact] on a bound store joins this transaction without a savepoint;
 * catching an application exception does not itself mark the transaction for rollback.
 * A separate call to [DataSource.transact] always opens an independent transaction, even
 * inside this block. There is no ambient transaction lookup.
 *
 * As with [withTransaction], a connection failure or cancellation during commit can leave
 * the outcome uncertain. Retries must be idempotent.
 */
suspend fun <R> DataSource.transact(block: suspend DocStoreTransaction.() -> R): R =
    withTransaction { connection ->
        val scope = DocStoreTransaction(this, connection)
        try {
            scope.block()
        } finally {
            scope.close()
        }
    }
