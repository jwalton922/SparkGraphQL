package org.jwalton922.sparkgraphql.graphql;

import graphql.execution.ExecutionPath;
import graphql.execution.ExecutionTypeInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.jwalton922.sparkgraphql.SparkGraphQLQueryEvaluator;
import org.jwalton922.sparkgraphql.RddOperatorIF;

/**
 *
 * @author jwalton922
 */
public class SparkDataFetcher implements DataFetcher {

    public static final String TYPE_FIELD = "FETCHER_TYPE";
    public static final String ID_JOINER = "_";
    private RddOperatorIF rddOperator;
    
    public SparkDataFetcher(RddOperatorIF rddOperator){
        this.rddOperator = rddOperator;
    }

    @Override
    public Object get(DataFetchingEnvironment dfe) {
        Object source = dfe.getSource();
        String type = GraphQLUtils.getGraphQLType(dfe.getFieldType());
        String fieldName = dfe.getFields().get(0).getName();
        if (GraphQLUtils.isScalarType(dfe.getFieldType())) {
            //we don't care about scalars since we are looking for objects to
            //create spark RDDs and join on
            return null;
        }
        //Query Evaluator creates SparkSession and puts it in context. retrieve it here        
        SparkSession spark = (SparkSession) ((Map) dfe.getContext()).get(SparkGraphQLQueryEvaluator.CONTEXT_SPARK_SESSION);
        if (source == null) {
            //no previous steps to query so need to create an RDD and store it in the context for any future joins
            //if query has children types that need to be fetched

            //create RDD for type
            JavaRDD<Map> rdd = rddOperator.createRDD(type, spark);
            //store rdd so it can be used for joins for any nested types
            ((Map) dfe.getContext()).put(SparkGraphQLQueryEvaluator.CONTEXT_SPARK_OUTPUT, rdd);
            //return a non null value so that query processing continues until the end
            if (GraphQLUtils.isArrayType(dfe.getFieldType())) {
                return Arrays.asList(new HashMap<>());
            } else {
                //this case should be unlikely because this project is meant for bulk exports
                return new HashMap<>();
            }
        } else {
            String parentType = GraphQLUtils.getGraphQLType(dfe.getParentType());
            List<String> executionPath = getPathStrings(dfe);
            
            JavaRDD innerTypeRdd = rddOperator.createRDD(type, spark);
            JavaPairRDD<String, Iterable<Map>> innerByForeignKey = rddOperator.mapToForeignKeyId(type, parentType, innerTypeRdd).groupByKey();

            JavaRDD<Map> rootRdd = (JavaRDD) ((Map) dfe.getContext()).get(SparkGraphQLQueryEvaluator.CONTEXT_SPARK_OUTPUT);
            JavaPairRDD<String, Map> rootRddById = rddOperator.mapToId(parentType, executionPath, rootRdd);
                        
            //perform left outer join between data so far with new data
            JavaRDD<Map> joinedRdd = rootRddById.leftOuterJoin(innerByForeignKey).map(tupleHero -> {
                Map rootObj = tupleHero._2()._1();
                List<Map> objsToUpdate = new ArrayList<>(Arrays.asList(rootObj));
                if (executionPath.size() > 2) {
                    objsToUpdate = getInnerSelections(objsToUpdate, executionPath, 1, executionPath.size() - 2);
                }
                Optional<Iterable<Map>> joinedObjList = tupleHero._2()._2();
                for (Map objToUpdate : objsToUpdate) {
                    if (joinedObjList.isPresent()) {
                        List<Map> list = new ArrayList<>();
                        for (Map map : joinedObjList.get()) {
                            list.add(map);
                        }
                        objToUpdate.put(fieldName, list);
                    }
                }
                return rootObj;
            });
            //store joined rdd into context for future steps
            ((Map) dfe.getContext()).put(SparkGraphQLQueryEvaluator.CONTEXT_SPARK_OUTPUT, joinedRdd);
            //return something so can continue with GraphQL Query
            if (GraphQLUtils.isArrayType(dfe.getFieldType())) {
                return Arrays.asList(new HashMap<>());
            } else {
                return new HashMap<>();
            }
        }
    }

    /**
     * Returns execution path which is the hierarchy of the GraphQL query up to
     * this point
     *
     * @param dfe
     * @return
     */
    private List<String> getPathStrings(DataFetchingEnvironment dfe) {
        ExecutionTypeInfo execInfo = dfe.getFieldTypeInfo();
        ExecutionPath execPath = execInfo.getPath();
        List<Object> execPathList = execPath.toList();
        List<String> retList = new ArrayList<>();
        for (Object obj : execPathList) {
            if (obj instanceof String) {
                retList.add((String) obj);
            }
        }

        return retList;
    }

    /**
     * Recursively select inner objects based on execution path
     *
     * @param maps
     * @param executionPaths
     * @param index
     * @param maxIndex
     * @return
     */
    public static List<Map> getInnerSelections(List<Map> maps, List<String> executionPaths, int index, int maxIndex) {
        List<Map> retList = new ArrayList<>();
        for (Map map : maps) {
            String path = executionPaths.get(index);
            if (map.containsKey(path)) {
                Object value = map.get(path);
                if (value == null) {
                    continue;
                }
                if (value instanceof Map) {
                    retList.add((Map) value);
                } else if (value instanceof List) {
                    List valueList = (List) value;
                    for (Object obj : valueList) {
                        if (obj != null && obj instanceof Map) {
                            retList.add((Map) obj);
                        }
                    }
                }
            }
        }
        index = index + 1;
        if (index > maxIndex) {
            return retList;
        } else {
            return getInnerSelections(retList, executionPaths, index, maxIndex);
        }
    }

}
