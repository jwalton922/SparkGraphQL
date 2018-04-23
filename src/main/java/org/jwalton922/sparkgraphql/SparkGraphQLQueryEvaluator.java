
package org.jwalton922.sparkgraphql;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author jwalton922
 */
public class SparkGraphQLQueryEvaluator {

    public static String CONTEXT_SPARK_SESSION = "sparkSession";    
    public static String CONTEXT_SPARK_OUTPUT = "sparkOutput";
    
    private GraphQL graphQL;
    private SparkSession spark;
    public SparkGraphQLQueryEvaluator(GraphQL graphQL, SparkSession spark){
        this.graphQL = graphQL;
        this.spark = spark;
    }
    
    public JavaRDD processQuery(String query){
        Map<String, Object> context = new HashMap<>();
        context.put(CONTEXT_SPARK_SESSION, spark);
        
        ExecutionInput input = ExecutionInput.newExecutionInput().query(query).context(context).build();
        ExecutionResult result = this.graphQL.execute(input);
        return (JavaRDD)context.get(CONTEXT_SPARK_OUTPUT);        
    }
}
