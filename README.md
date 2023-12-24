[![Process Pull Request](https://github.com/formation-res/pg-docstore/actions/workflows/pr_master.yaml/badge.svg)](https://github.com/formation-res/pg-docstore/actions/workflows/pr_master.yaml)

Pg-docstore is a kotlin library that allows you to use postgres as a json document store from Kotlin.

## Why

Document stores are very useful in some applications and while popular in the nosql world, sql databases like
postgres provide a lot of nice functionality ant performance and is therefore a popular choice for data storage. 
Using postgres as a document store makes a lot of sense. Transactionality ensures that your data is safe. You can use any
of a wide range of managed and cloud based service providers or just host it yourself.

At FORMATION, we use pg-docstore to store millions of documents of different types. We use Elasticsearch for querying, aggregations, and other functionality and therefore have no need
for the elaborate querying support in databases. But we do like to keep our data safe, which is why we like postgres. 
Additionally, we like having a clear separation between what we store and what we query on. So, our architecture includes
an ETL pipeline that builds our search index from the raw data in pg-docstore.

## Features

- document store with crud operations for storing and retrieving json documents
- update function that retrieves, applies your lambda to the retrieved document, and then stores in a transaction.
- serialization is done using kotlinx.serialization
- efficient bulk insert/updates
- efficient querying and dumping of the content of the store with database scrolls. We use this for our ETL pipeline.
- nice Kotlin API with suspend functions, flows, strong typing, etc. 

This library builds on jasync-postgresql, which is one of the few database drivers out there that is written in Kotlin
and that uses non blocking IO. 

## Future work

As FORMATION grows, we will no doubt need more features. Some features that come to mind are sharding, utilizing some of
the json features in postgres, or even it's text search and geospatial features.

## Development status

This is a relatively new project; so there may be some bugs, design flaws, etc. However, I've implemented similar 
stores many times before in past projects and I think I know what I'm doing. If it works for us, it might also work for you. 

Give it a try!

## License and contributing

The code is provided as is under the [MIT](LICENSE.md). If you are planning to make a contribution, please 
reach out via the issue tracker first.
