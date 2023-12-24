package com.jillesvangurp.pgdocstore

import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class TestModel(val property: String)

@Serializable
data class TestModelWithId(val property: String, val id: String = UUID.randomUUID().toString())
