package io.polyglotted.eswrapper.indexing;

import com.google.gson.*;

import java.lang.reflect.Type;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class IndexSerializer {

    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(TypeMapping.class, new TypeMappingSerializer())
            .registerTypeAdapter(FieldMapping.Builder.class, new FieldMappingSerializer())
            .create();

    private static final class TypeMappingSerializer implements JsonSerializer<TypeMapping> {
        @Override
        public JsonElement serialize(TypeMapping mapping, Type type, JsonSerializationContext context) {
            JsonObject mainType = new JsonObject();
            if (mapping.strict) mainType.addProperty("dynamic", "strict");

            JsonObject source = new JsonObject();
            if (!mapping.storeSource) {
                mainType.add("_source", source);
                source.addProperty("enabled", false);

            } else if (!mapping.sourceIncludes.isEmpty()) {
                mainType.add("_source", source);
                source.add("includes", context.serialize(mapping.sourceIncludes));
            }

            JsonObject all = new JsonObject();
            mainType.add("_all", all);
            all.addProperty("enabled", true);
            all.addProperty("analyzer", "all_analyzer");

            if(!mapping.metaData.isEmpty())
                mainType.add("_meta", context.serialize(mapping.metaData));

            if (!mapping.transformScripts.isEmpty()) {
                if (mapping.transformScripts.size() > 1)
                    mainType.add("transform", context.serialize(mapping.transformScripts));
                else
                    mainType.add("transform", context.serialize(mapping.transformScripts.get(0)));
            }

            JsonObject properties = new JsonObject();
            mainType.add("properties", properties);
            for (FieldMapping field : mapping.fieldMappings)
                properties.add(field.field, context.serialize(field.mapping));

            JsonObject result = new JsonObject();
            result.add(mapping.type, mainType);
            return result;
        }
    }

    private static final class FieldMappingSerializer implements JsonSerializer<FieldMapping.Builder> {
        @Override
        public JsonElement serialize(FieldMapping.Builder builder, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            object.addProperty("type", toLowerCase(checkNotNull(builder.type(), "type cannot be null")));
            if (builder.indexed() != null) object.addProperty("index", toLowerCase(builder.indexed()));
            if (builder.stored() != null) object.addProperty("store", builder.stored());
            if (builder.includeInAll() != null) object.addProperty("include_in_all", builder.includeInAll());
            if (builder.docValues() != null) object.addProperty("doc_values", builder.docValues());
            return object;
        }
    }

    private static String toLowerCase(Object value) {
        return String.valueOf(value).toLowerCase();
    }
}
