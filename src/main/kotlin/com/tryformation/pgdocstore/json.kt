package com.tryformation.pgdocstore

import com.jillesvangurp.serializationext.DEFAULT_JSON
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json

inline fun <reified T> DocStoreEntry.document(json: Json = DEFAULT_JSON): T = json.decodeFromString<T>(this.json)
