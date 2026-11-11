package com.tryformation.pgdocstore

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Reads documents by storage ID in bounded batches. Missing IDs are omitted and each requested
 * ID is looked up once. The order of results within each batch is unspecified.
 *
 * The flow is cold. For an unbound store, each lookup returns its connection before emitting
 * results. Collect a flow from a transaction-bound store inside the transaction.
 */
fun <T : Any> IDocStore<T>.multiGetByIdFlow(ids: Iterable<String>, batchSize: Int = 100): Flow<T> =
    multiGetFlow(ids, batchSize, ::multiGetById)

/**
 * Like [multiGetByIdFlow], with the storage ID and metadata included in each result.
 */
fun <T : Any> IDocStore<T>.multiGetEntryByIdFlow(
    ids: Iterable<String>,
    batchSize: Int = 100,
): Flow<DocStoreEntry> = multiGetFlow(ids, batchSize, ::multiGetEntryById)

private fun <T> multiGetFlow(
    ids: Iterable<String>,
    batchSize: Int,
    fetch: suspend (List<String>) -> List<T>,
): Flow<T> {
    require(batchSize > 0) { "batchSize must be positive" }
    return flow {
        for (batch in ids.asSequence().distinct().chunked(batchSize)) {
            for (item in fetch(batch)) emit(item)
        }
    }
}
