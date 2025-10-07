package com.tryformation.pgdocstore

import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class TestModel(val property: String)

@Serializable
data class TestModelWithId(val title: String, val id: String = UUID.randomUUID().toString())

@Serializable
data class TaggedModel(val title: String, val tags: List<String> = listOf(), val id:String = UUID.randomUUID().toString())
