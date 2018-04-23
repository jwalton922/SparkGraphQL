
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import graphql.language.FieldDefinition;
import graphql.language.Node;
import graphql.language.TypeDefinition;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.jwalton922.sparkgraphql.SparkGraphQLQueryEvaluator;
import org.jwalton922.sparkgraphql.graphql.SparkDataFetcher;
import org.jwalton922.sparkgraphql.sample.SampleRddOperator;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author uwaltj6
 */
public class SampleTest {

    private static SparkSession spark;
    private static GraphQL graphQL;
    private static ObjectMapper mapper = new ObjectMapper();
    private static SampleRddOperator sampleOperator = new SampleRddOperator();
    @BeforeClass
    public static void init() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("SparkGraphQL");
        spark = SparkSession.builder().config(conf).getOrCreate();
        graphQL = buildGraphQL();
    }

    private static GraphQL buildGraphQL() {
        String schema = "type Post {id: ID! comments: [Comment], creator: User}\n";
        schema += "type Comment {id: ID! madeBy: User post: Post}\n";
        schema += "type User {id: ID! name: String, comments: [Comment], posts: [Post]}\n";
        schema += "type Query {postsQuery: [Post]}";
        schema += "schema {query: Query}";
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        SchemaParser schemaParser = new SchemaParser();
        TypeDefinitionRegistry typeRegistry = schemaParser.parse(schema);

        RuntimeWiring.Builder runtimeBuilder = RuntimeWiring.newRuntimeWiring();
        SparkDataFetcher sparkFetcher = new SparkDataFetcher(sampleOperator);
        for (String typeName : typeRegistry.types().keySet()) {
            TypeRuntimeWiring.Builder typeBuilder = TypeRuntimeWiring.newTypeWiring(typeName);

            TypeDefinition typeDef = typeRegistry.getType(typeName).get();
            List<Node> typeFields = typeDef.getChildren();

            for (Node node : typeFields) {
                if (node instanceof FieldDefinition) {
                    FieldDefinition fieldDef = (FieldDefinition) node;
                    typeBuilder = typeBuilder.dataFetcher(fieldDef.getName(), sparkFetcher);
                }
            }
            runtimeBuilder = runtimeBuilder.type(typeBuilder.build());
        }
        RuntimeWiring runtimeWiring = runtimeBuilder.build();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
        GraphQL graphQLA = GraphQL.newGraphQL(graphQLSchema).build();

        return graphQLA;
    }

    @AfterClass
    public static void close() {
        spark.close();
    }

    @Test
    public void runTest() {
        

        SparkGraphQLQueryEvaluator queryEvaluator = new SparkGraphQLQueryEvaluator(graphQL, spark);
        //String query = "{ postsQuery { id creator { id name } comments { id madeBy { id name } } } }";
        String query = "{ postsQuery { id creator { id name } comments { id madeBy { id name } } } }";
        JavaRDD<Map> output = queryEvaluator.processQuery(query);
        List<Map> results = output.collect();
        
        results.forEach(map -> {
            try  {
                String mapJson = mapper.writeValueAsString(map);
                System.out.println("Output map: "+mapJson);
            } catch(Exception e){
            }
        });
        System.out.println("Total of "+results.size()+" results");
        Assert.assertTrue(results.size() == 1);
        
        
    }
}
