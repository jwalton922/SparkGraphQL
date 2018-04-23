package org.jwalton922.sparkgraphql.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.jwalton922.sparkgraphql.RddOperatorIF;
import org.jwalton922.sparkgraphql.graphql.SparkDataFetcher;
import scala.Tuple2;

/**
 *
 * @author jwalton922
 */
public class SampleRddOperator implements RddOperatorIF{
    List<Map> posts = Arrays.asList(
            new HashMap<>(ImmutableMap.builder().put("id", "post1").put("User_id", "user1").build())
    );
    List<Map> comments = Arrays.asList(
            new HashMap<>(ImmutableMap.builder().put("id", "comment1").put("User_id", "user2").put("Post_id", "post1").build()),
            new HashMap<>(ImmutableMap.builder().put("id", "comment2").put("User_id", "user1").put("Post_id", "post1").build())
    );
    
    List<Map> users = Arrays.asList(
            new HashMap<>(ImmutableMap.builder().put("id", "user1").put("name", "Josh").put("Post_id","post1").put("Comment_id", "comment2").build()),
            new HashMap<>(ImmutableMap.builder().put("id", "user2").put("name", "Joe").put("Comment_id", "comment1").build())            
    );
    @Override
    public JavaRDD<Map> createRDD(String type, SparkSession spark) {
        if(type.equalsIgnoreCase("post")){
            return new JavaSparkContext(spark.sparkContext()).parallelize(posts);
        } else if(type.equalsIgnoreCase("comment")){
            return new JavaSparkContext(spark.sparkContext()).parallelize(comments);
        } else if(type.equalsIgnoreCase("user")){
            return new JavaSparkContext(spark.sparkContext()).parallelize(users);
        } else {
            throw new RuntimeException("Do notk now how to retrieve data for type: "+type);
        }
    }

    @Override
    public JavaPairRDD<String, Map> mapToForeignKeyId(String type, String foreignType, JavaRDD<Map> rdd) {
        List<String> foreignKeyIdFields = Arrays.asList(foreignType+"_id");
        return rdd.flatMapToPair(map -> {
            List<Tuple2<String, Map>> retList = new ArrayList<>();
            List<String> idParts = new ArrayList<>();
            for (String idField : foreignKeyIdFields) {
                Object idValue = map.get(idField);
                if (idValue != null) {
                    idParts.add(idValue.toString());
                }
            }

            if (idParts.size() == foreignKeyIdFields.size()) {
                String foreignKeyId = StringUtils.join(idParts, SparkDataFetcher.ID_JOINER);
                retList.add(new Tuple2<>(foreignKeyId, map));
            }
            return retList.iterator();
        });
    }

    @Override
    public JavaPairRDD<String, Map> mapToId(String type, List<String> executionPath, JavaRDD<Map> rdd) {
        List<String> idFields = Arrays.asList("id");
        return rdd.flatMapToPair(inputMap -> {
            List<Tuple2<String, Map>> retList = new ArrayList<>();
            List<String> idParts = new ArrayList<>();
            List<Map> mapsToGetIdsFrom = new ArrayList<>(Arrays.asList(inputMap));
            //each element in execution path is how deep we are in query
            //if path is > 2, means path is [query $FirstObjClass $secondObjClass...$nthObjClass]
            if (executionPath.size() > 2) {
                //start at 1 since first one is query, max is size -2 since sice -2 should be previous step
                mapsToGetIdsFrom = SparkDataFetcher.getInnerSelections(mapsToGetIdsFrom, executionPath, 1, executionPath.size() - 2);
            }

            for (Map map : mapsToGetIdsFrom) {
                for (String idField : idFields) {
                    Object idValue = map.get(idField);
                    if (idValue != null) {
                        idParts.add(idValue.toString());
                    }
                }

                if (idParts.size() == idFields.size()) {
                    String id = StringUtils.join(idParts, SparkDataFetcher.ID_JOINER);
                    retList.add(new Tuple2<>(id, inputMap));
                }
            }
            return retList.iterator();
        });
    }
    
}
