package com.jillesvangurp.pgdocstore

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private object Done
private object TimerExpired

/**
 * Implements chunking for flows.
 *
 * There are some subtleties to chunking flows that are the main reason this was never added in co-routines. This
 * implementation deals with this in an opinionated way. The emitted chunks are not guaranteed to be exactly [chunkSize].
 *
 * [chunkSize] controls the maximum size of the chunk. [delayMillis] is used to
 * ensure items are emitted regularly even if the full [chunkSize] is not reached. This gives you a mechanism to
 * continuously emit chunks and guarantee items don't sit in the buffer waiting to be emitted endlessly when the
 * upstream pauses emitting new items for whatever reason.
 */
@OptIn(ExperimentalCoroutinesApi::class)
fun <T> Flow<T>.chunked(
    chunkSize: Int = 100,
    delayMillis: Duration = 250.milliseconds,
): Flow<List<T>> {
    require(chunkSize > 0) { "'chunkSize' must be positive: $chunkSize" }

    val upstream: Flow<Any?> = this

    return flow {

        //We use this to control how often we emit elements.
        val timerToggleFlow = MutableSharedFlow<Boolean?>()
        var buffer = mutableListOf<T>()

        // merge the timer flow and upstream
        // the resulting flow receives elements from upstream and TimerExpired or Done objects
        // we use those to control when we emit chunks
        merge(
            upstream.onCompletion {
                // signals that we don't expect more elements
                emit(Done)
            },
            // the timer flow is actioned here
            timerToggleFlow
                // null is our stop condition
                .takeWhile { it != null }
                // otherwise sleep and emit TimerExpired or an emptyFlow
                .flatMapLatest { enabled ->
                    if (enabled!!)
                        flow {
                            delay(delayMillis)
                            emit(TimerExpired)
                        }
                    else
                        emptyFlow()
                }
        )
            .collect { element ->
                when (element) {
                    Done -> {
                        // we're done, emit remaining elements
                        if (buffer.isNotEmpty()) {
                            emit(buffer)
                            buffer = mutableListOf()
                        }

                        // stop the timer flow
                        timerToggleFlow.emit(null)
                    }

                    TimerExpired -> {
                        // we've waited for the specified time
                        // emit whatever is in the buffer
                        if (buffer.isNotEmpty()) {
                            emit(buffer)
                            buffer = mutableListOf()
                        }
                    }

                    else -> {
                        // the element is from upstream, add it to the buffer
                        @Suppress("UNCHECKED_CAST")
                        buffer.add(element as T)

                        // if our buffer is large enough, emit a chunk
                        if (buffer.size >= chunkSize) {
                            emit(buffer)
                            buffer = mutableListOf()
                            // stop the timer until we receive more elements
                            timerToggleFlow.emit(false)
                        } else if (buffer.size == 1) {
                            // wait for more elements
                            timerToggleFlow.emit(true)
                        }
                    }
                }
            }
    }
}