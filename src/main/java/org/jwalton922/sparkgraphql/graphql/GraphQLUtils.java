package org.jwalton922.sparkgraphql.graphql;

import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author jwalton922
 */
public class GraphQLUtils {
     static Set<String> scalarTypes = new HashSet<>(Arrays.asList(
            "string",
            "long",
            "int",
            "boolean",
            "float",
            "id"
    ));
    public static boolean isScalarType(Type type){
        
        if(type instanceof NonNullType){
            return isScalarType(((NonNullType)type).getType());
        } else if(type instanceof TypeName){
            String name = ((TypeName) type).getName();
            return scalarTypes.contains(name.toLowerCase());
        }
        else if (type instanceof GraphQLNonNull) {
            return isScalarType(((GraphQLNonNull) type).getWrappedType());
        } else if (type instanceof GraphQLList) {
            return isScalarType(((GraphQLList) type).getWrappedType());
        } else {
            return type instanceof GraphQLScalarType;
        }
    }
    
    public static boolean isScalarType(GraphQLType type){
        if (type instanceof GraphQLNonNull) {
            return isScalarType(((GraphQLNonNull) type).getWrappedType());
        } else if (type instanceof GraphQLList) {
            return isScalarType(((GraphQLList) type).getWrappedType());
        } else {
            return type instanceof GraphQLScalarType;
        }
    }
    
    public static String getGraphQLType(GraphQLType type) {
        if (type instanceof GraphQLNonNull) {
            return getGraphQLType(((GraphQLNonNull) type).getWrappedType());
        } else if (type instanceof GraphQLList) {
            return getGraphQLType(((GraphQLList) type).getWrappedType());
        } else {
            return type.getName();
        }
    }
    
    public static boolean isArrayType(GraphQLType type){
        if(type instanceof GraphQLNonNull){
            return isArrayType(((GraphQLNonNull) type).getWrappedType());
        }
        
        return type instanceof GraphQLList;
    }
    
}
