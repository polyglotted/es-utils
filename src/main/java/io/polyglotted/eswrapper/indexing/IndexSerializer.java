package io.polyglotted.eswrapper.indexing;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.polyglotted.pgmodel.search.index.FieldMapping;
import io.polyglotted.pgmodel.search.index.Indexed;
import io.polyglotted.pgmodel.search.index.Script;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableSortedSet.copyOf;
import static com.google.common.collect.Iterables.concat;
import static io.polyglotted.eswrapper.ElasticConstants.ALL_META;
import static io.polyglotted.eswrapper.ElasticConstants.META_META;
import static io.polyglotted.eswrapper.ElasticConstants.PARENT_META;
import static io.polyglotted.eswrapper.ElasticConstants.SOURCE_META;
import static io.polyglotted.pgmodel.search.index.FieldType.OBJECT;
import static io.polyglotted.pgmodel.search.index.FieldType.STRING;
import static io.polyglotted.pgmodel.search.index.HiddenFields.hiddenFields;
import static io.polyglotted.pgmodel.search.index.Indexed.NOT_ANALYZED;

public abstract class IndexSerializer {

    public static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
       .registerTypeAdapter(TypeMapping.class, new TypeMappingSerializer())
       .registerTypeAdapter(FieldMapping.class, new FieldMappingSerializer())
       .registerTypeAdapter(Script.class, new ScriptMappingSerializer())
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
            if (!mapping.storeSource) {
                mainType.add(SOURCE_META, source);
                source.addProperty("enabled", false);

            } else if (!mapping.sourceIncludes.isEmpty()) {
                mainType.add(SOURCE_META, source);
                source.add("includes", context.serialize(mapping.sourceIncludes));
            }

            if (mapping.parent != null) {
                JsonObject parent = new JsonObject();
                parent.addProperty("type", mapping.parent);
                mainType.add(PARENT_META, parent);
            }

            JsonObject all = new JsonObject();
            mainType.add(ALL_META, all);
            all.addProperty("enabled", mapping.allAnalyzer != null);
            if (mapping.allAnalyzer != null) all.addProperty("analyzer", mapping.allAnalyzer);

            if (!mapping.meta.isEmpty())
                mainType.add(META_META, context.serialize(mapping.meta));

            if (!mapping.scripts.isEmpty()) {
                if (mapping.scripts.size() > 1)
                    mainType.add("transform", context.serialize(mapping.scripts));
                else
                    mainType.add("transform", context.serialize(mapping.scripts.get(0)));
            }

            if (!mapping.fieldMappings.isEmpty()) {
                JsonObject properties = new JsonObject();
                mainType.add("properties", properties);
                for (FieldMapping field : copyOf(concat(hiddenFields(), mapping.fieldMappings)))
                    properties.add(field.field, context.serialize(field));
            }

            JsonObject result = new JsonObject();
            result.add(mapping.type, mainType);
            return result;
        }
    }

    private static final class FieldMappingSerializer implements JsonSerializer<FieldMapping> {
        @Override
        public JsonElement serialize(FieldMapping field, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            if (field.type != OBJECT) object.addProperty("type", toLowerCase(field.type));
            object.addProperty("index", decorateIndex(field));
            object.addProperty("analyzer", field.analyzer);
            object.addProperty("store", field.stored);
            object.addProperty("doc_values", field.docValues);
            object.addProperty("copy_to", field.copyTo);
            field.type.extra(object);
            object.addProperty("include_in_all", field.includeInAll);
            field.argsMap.entrySet().forEach(arg -> object.add(arg.getKey(), context.serialize(arg.getValue())));
            if (field.hasFields()) object.add("fields", context.serialize(field.properties));
            else if (field.hasProperties()) object.add("properties", context.serialize(field.properties));
            return object;
        }
    }

    private static final class ScriptMappingSerializer implements JsonSerializer<Script> {
        @Override
        public JsonElement serialize(Script script, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            object.addProperty("script", script.script);
            if (!script.parameters.isEmpty()) object.add("params", context.serialize(script.parameters));
            if (script.lang != null) object.addProperty("lang", script.lang);
            return object;
        }
    }

    private static String toLowerCase(Object value) {
        return String.valueOf(value).toLowerCase();
    }

    private static String decorateIndex(FieldMapping field) {
        Indexed indexed = (field.type != STRING && field.indexed == NOT_ANALYZED) ? null : field.indexed;
        return indexed == null ? null : toLowerCase(indexed);
    }
}
