# SparkGraphQL
The goal of this project was to test the possibility of providing a bulk export of a [GraphQL](https://www.graphql.org) query that uses [Spark](https://spark.apache.org/).  This type of export might be useful in a data lake setting as data is frequently aggregated or exported for analysis. A GraphQL API could be written to expose the data in quantities that can be displayed in a UI, and then this project could export the data for offline, analytical type processing.

This project uses the Java GraphQL implementation to parse the query, and call functions that perform Spark map functions for each step of the query. Basically it performs a series of left outer joins for each nested query layer. See the sample case below. Note that it currently doesn't whitelist the fields like a real GraphQL implementation since this export is likely an expensive operation and it shouldn't be repeated to grab additional fields.

## Usage
1. Implement the [RddOperatorIF](https://github.com/jwalton922/SparkGraphQL/blob/master/src/main/java/org/jwalton922/sparkgraphql/RddOperatorIF.java) interface. 
	* These methods will load the data for a type into a Spark RDD as well as PairRDD so that joins can happen. The GraphQL schema needs to wire the SparkDataFetcher to any GraphQL types
2. Wire the [SparkDataFetcher](https://github.com/jwalton922/SparkGraphQL/blob/master/src/main/java/org/jwalton922/sparkgraphql/graphql/SparkDataFetcher.java) to each GraphQL type and its fields in the schema.
3. Use [SparkGraphQLQueryEvaluator](https://github.com/jwalton922/SparkGraphQL/blob/master/src/main/java/org/jwalton922/sparkgraphql/SparkGraphQLQueryEvaluator.java) to evaluate a GraphQL query.
4. Do something with the returned JavaRDD (ie output to S3 or hdfs).

### Sample
See the [test class](https://github.com/jwalton922/SparkGraphQL/blob/master/src/test/java/SampleTest.java). It uses a schema similar to that below:
```
type Post {
	creator: User
	comments: [Comment]
}
type Comment {
	post: Post
	madeBy: User
}

type User {
	name: String
}

type Query {
	posts: [Posts]
}
schema {
	query: Query
}
```
And uses a query like:
```
{
	postsQuery {
		creator {
			name
		} 
		comments {		
			madeBy {
				name 
			} 
		} 
	} 
}
```

A sample output line looks like:
```
{
  "creator": [
    {
      "Comment_id": "comment2",
      "name": "Josh",
      "id": "user1",
      "Post_id": "post1"
    }
  ],
  "comments": [
    {
      "madeBy": [
        {
          "Comment_id": "comment1",
          "name": "Joe",
          "id": "user2"
        }
      ],
      "id": "comment1",
      "User_id": "user2",
      "Post_id": "post1"
    },
    {
      "madeBy": [
        {
          "Comment_id": "comment1",
          "name": "Joe",
          "id": "user2"
        }
      ],
      "id": "comment2",
      "User_id": "user1",
      "Post_id": "post1"
    }
  ],
  "id": "post1",
  "User_id": "user1"
}
```
