package org.jwalton922.sparkgraphql;

import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author jwalton922
 */
public interface RddOperatorIF {
    
    /**
     * Give GraphQL data type, use spark to create RDD
     * @param type GraphQL Type to read in
     * @param spark Spark Session
     * @return 
     */
    public JavaRDD<Map> createRDD(String type, SparkSession spark);
    /**
     * Perform a flat map to pair each object in input RDD to an foreign key to be used as the right side in a Left Outer Join
     * @param type GraphQL type of objects on right side of join
     * @param foreignType GraphQL type of objects on left side join, this would be the parent type in a GraphQL query
     * @param rdd input RDD
     * @return 
     */
    public JavaPairRDD<String, Map> mapToForeignKeyId(String type, String foreignType, JavaRDD<Map> rdd);
   
    /**
     * Perform a flat map to pair each object in input RDD to its ID. Will be used as left side of Left Outer join. 
     * As the GraphQL query is traversed, this mapping to ID could be an object with nested data so the execution
     * path is necessary to select down to the object where the ids are stored.
     * @param type GraphQL type of input RDD
     * @param executionPath
     * @param rdd
     * @return 
     */
    public JavaPairRDD<String, Map> mapToId(String type, List<String> executionPath,JavaRDD<Map> rdd);
}
