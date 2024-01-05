package com.tryformation.pgdocstore

class DocumentNotFoundException(val id: String): Exception("document does not exist: $id")