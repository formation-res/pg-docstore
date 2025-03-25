package com.tryformation.pgdocstore

import kotlinx.datetime.Instant

data class DocStoreEntry(
    val id: String,
    val createdAt: Instant,
    val updatedAt: Instant,
    val json: String,
    val tags: List<String>?,
    val text: String?,
    val similarity: Float? = null,
)