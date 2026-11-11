package com.tryformation.pgdocstore

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.random.nextULong

class MultiGetFlowTest : DbTestBase() {
    @Test
    fun shouldReadIdsLazilyInBatches() = coRun {
        val store = DocStore(db, TestModelWithId.serializer(), tableName)
        val first = TestModelWithId(Random.nextULong().toString())
        val later = TestModelWithId(Random.nextULong().toString())
        val missing = Random.nextULong().toString()
        val ids = listOf(first.id, missing, later.id, first.id)
        val results = store.multiGetByIdFlow(ids, batchSize = 2)
        store.create(first)

        results.onEach {
            if (it.id == first.id) store.create(later)
        }.toList().map { it.id } shouldBe listOf(first.id, later.id)
        store.multiGetEntryByIdFlow(ids, batchSize = 1).toList().map { it.id } shouldBe listOf(first.id, later.id)
        store.multiGetByIdFlow(emptyList()).toList() shouldBe emptyList()
        store.delete(first.id)
        store.delete(later.id)
    }
}
