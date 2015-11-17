package io.polyglotted.eswrapper.indexing;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static io.polyglotted.eswrapper.ElasticConstants.ALL_META;
import static io.polyglotted.eswrapper.ElasticConstants.META_META;
import static io.polyglotted.eswrapper.ElasticConstants.PARENT_META;
import static io.polyglotted.eswrapper.ElasticConstants.SOURCE_META;
import static io.polyglotted.eswrapper.indexing.FieldType.OBJECT;

public abstract class IndexSerializer {

    public static final Gson GSON = new GsonBuilder()
       .registerTypeAdapter(TypeMapping.class, new TypeMappingSerializer())
       .registerTypeAdapter(FieldMapping.Builder.class, new FieldMappingSerializer())
       .registerTypeAdapter(TransformScript.class, new ScriptMappingSerializer())
       .create();
    public static final Type LIST_TYPE = new TypeToken<List<Map<String, Object>>>() {}.getType();

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserMap(String json) {
        return GSON.fromJson(json, Map.class);
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> deserList(String json) {
        return GSON.fromJson(json, LIST_TYPE);
    }

    private static final class TypeMappingSerializer implements JsonSerializer<TypeMapping> {
        @Override
        public JsonElement serialize(TypeMapping mapping, Type type, JsonSerializationContext context) {
            JsonObject mainType = new JsonObject();
            if (mapping.strict) mainType.addProperty("dynamic", "strict");

            JsonObject source = new JsonObject();
            if (!mapping.store) {
                mainType.add(SOURCE_META, source);
                source.addProperty("enabled", false);

            } else if (!mapping.includes.isEmpty()) {
                mainType.add(SOURCE_META, source);
                source.add("includes", context.serialize(mapping.includes));
            }

            if (mapping.parent != null) {
                JsonObject parent = new JsonObject();
                parent.addProperty("type", mapping.parent);
                mainType.add(PARENT_META, parent);
            }

            if (mapping.all != null) {
                JsonObject all = new JsonObject();
                mainType.add(ALL_META, all);
                all.addProperty("enabled", mapping.all);
                if (mapping.analyzer != null) all.addProperty("analyzer", mapping.analyzer);
            }

            if (!mapping.meta.isEmpty())
                mainType.add(META_META, context.serialize(mapping.meta));

            if (!mapping.scripts.isEmpty()) {
                if (mapping.scripts.size() > 1)
                    mainType.add("transform", context.serialize(mapping.scripts));
                else
                    mainType.add("transform", context.serialize(mapping.scripts.get(0)));
            }

            if (!mapping.mappings.isEmpty()) {
                JsonObject properties = new JsonObject();
                mainType.add("properties", properties);
                for (FieldMapping field : FieldMapping.PRIVATE_FIELDS)
                    properties.add(field.field, context.serialize(field.mapping));
                for (FieldMapping field : mapping.mappings)
                    properties.add(field.field, context.serialize(field.mapping));
            }

            JsonObject result = new JsonObject();
            result.add(mapping.type, mainType);
            return result;
        }
    }

    private static final class FieldMappingSerializer implements JsonSerializer<FieldMapping.Builder> {
        @Override
        public JsonElement serialize(FieldMapping.Builder builder, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            if (builder.type() != OBJECT) object.addProperty("type", toLowerCase(builder.type()));
            if (builder.indexed() != null) object.addProperty("index", toLowerCase(builder.indexed()));
            if (builder.analyzer() != null) object.addProperty("analyzer", builder.analyzer());
            if (builder.stored() != null) object.addProperty("store", builder.stored());
            if (builder.docValues() != null) object.addProperty("doc_values", builder.docValues());
            if (builder.includeInAll() != null) object.addProperty("include_in_all", builder.includeInAll());
            builder.extraProps().entrySet().forEach(extra -> object.add(extra.getKey(), context.serialize(extra.getValue())));
            if (builder.hasProperties()) object.add("properties", context.serialize(builder.properties()));
            return object;
        }
    }

    private static final class ScriptMappingSerializer implements JsonSerializer<TransformScript> {
        @Override
        public JsonElement serialize(TransformScript script, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            object.addProperty("script", script.script);
            if (!script.params.isEmpty()) object.add("params", context.serialize(script.params));
            if (script.lang != null) object.addProperty("lang", script.lang);
            return object;
        }
    }

    private static String toLowerCase(Object value) {
        return String.valueOf(value).toLowerCase();
    }
}
