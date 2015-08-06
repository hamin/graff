GRAFF - A Social Graph API
====

![Gopher](https://blog.golang.org/gopher/gopher.png)![Glimpse](http://a5.mzstatic.com/us/r30/Purple3/v4/54/db/6d/54db6dd9-b62f-bd12-6cd2-9aff80270a28/icon175x175.png) 


### Getting Started

1. Install Goop from outside the proejct: `go get github.com/nitrous-io/goop`

2. Run `goop install`

3. Build worker `goop go build worker.go`


### Neo4J Indexes

[Neo4J Schema Index Docs](http://neo4j.com/docs/stable/query-schema-index.html)

We have to create the following indexs (these are nodes that we directly query with the 'WHERE' clause):

```
CREATE INDEX ON :InstagramUser(InstagramID)
CREATE INDEX ON :InstagramLocation(Username)
```

```
CREATE INDEX ON :InstagramLocation(InstagramID)
```

