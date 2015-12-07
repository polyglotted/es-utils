package io.polyglotted.eswrapper.services;

import io.polyglotted.esmodel.api.index.Alias;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.TypeMapping;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.polyglotted.eswrapper.indexing.TypeMapping.forcedMappingJson;
import static io.polyglotted.eswrapper.services.ModelIndexUtil.aliasActions;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.indexAliasesRequest;
import static org.elasticsearch.client.Requests.updateSettingsRequest;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.xContent;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@RequiredArgsConstructor
public final class AdminWrapper {
    private final Client client;

    public boolean indexExists(String... indices) {
        IndicesAdminClient indicesAdmin = client.admin().indices();
        return indicesAdmin.exists(new IndicesExistsRequest(indices)).actionGet().isExists();
    }

    public String createIndex(IndexSetting setting, String index) {
        createIndex(setting, Collections.emptyList(), index);
        return getIndex(index);
    }

    public void createIndex(IndexSetting setting, List<String> aliases, String... indices) {
        for (String index : indices) {
            if (indexExists(index)) continue;

            CreateIndexRequest request = createIndexRequest(index).settings(setting.createJson());
            for (String alias : aliases) {
                request.alias(new org.elasticsearch.action.admin.indices.alias.Alias(alias));
            }
            AcknowledgedResponse response = client.admin().indices().create(request).actionGet();
            checkState(response.isAcknowledged(), "unable to create index for " + index);
        }
    }

    public MetaData getMeta(String... indices) {
        ClusterStateResponse stateResponse = client.admin().cluster().prepareState()
           .setIndices(indices).execute().actionGet();
        return stateResponse.getState().metaData();
    }

    @SneakyThrows(IOException.class)
    public String getIndex(String... indices) {
        MetaData indexMetaDatas = getMeta(indices);
        XContentBuilder builder = new XContentBuilder(xContent(JSON), new BytesStreamOutput());

        builder.startArray();
        ImmutableOpenMap<String, IndexMetaData> getIndices = indexMetaDatas.getIndices();
        Iterator<String> indexIt = getIndices.keysIt();
        while (indexIt.hasNext()) {
            String index = indexIt.next();
            IndexMetaData metaData = getIndices.get(index);
            builder.startObject();
            builder.startObject(index);

            builder.startObject(new XContentBuilderString("aliases"));
            ImmutableOpenMap<String, AliasMetaData> aliases = metaData.aliases();
            Iterator<String> aIt = aliases.keysIt();
            while (aIt.hasNext()) {
                AliasMetaData alias = aliases.get(aIt.next());
                AliasMetaData.Builder.toXContent(alias, builder, EMPTY_PARAMS);
            }
            builder.endObject();

            builder.startObject(new XContentBuilderString("settings"));
            Settings settings = metaData.settings();
            settings.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            builder.endObject();
            builder.endObject();
        }
        builder.endArray();
        return builder.bytes().length() > 2 ? convertToJson(builder.bytes(), false) : builder.bytes().toUtf8();
    }

    public void updateSetting(IndexSetting setting, String... indices) {
        checkState(indexExists(indices), "one or more index does not exist " + Arrays.toString(indices));
        UpdateSettingsRequest settingsRequest = updateSettingsRequest(indices).settings(setting.updateJson());
        AcknowledgedResponse response = client.admin().indices().updateSettings(settingsRequest).actionGet();
        checkState(response.isAcknowledged(), "unable to update settings for " + Arrays.toString(indices));
    }

    public void updateAliases(Alias... aliases) {
        IndicesAliasesRequest aliasesRequest = indexAliasesRequest();
        for (Alias alias : aliases) {
            aliasesRequest.addAliasAction(aliasActions(alias));
        }
        AcknowledgedResponse response = client.admin().indices().aliases(aliasesRequest).actionGet();
        checkState(response.isAcknowledged(), "unable to update aliases");
    }

    public boolean typeExists(String index, String... types) {
        IndicesAdminClient indicesAdmin = client.admin().indices();
        TypesExistsRequest request = new TypesExistsRequest(new String[]{index}, types);
        return indicesAdmin.typesExists(request).actionGet().isExists();
    }

    public String createType(TypeMapping mapping) {
        checkState(indexExists(mapping.index), "create the index before creating type");
        if (!typeExists(mapping.index, mapping.type)) {

            PutMappingResponse response = client.admin().indices().putMapping(
               new PutMappingRequest(mapping.index).type(mapping.type).source(mapping.mappingJson())).actionGet();
            checkState(response.isAcknowledged(), "could not create type " + mapping.type);
            forceRefresh();
        }
        return getMapping(mapping.index, mapping.type);
    }

    public void createForcedType(String index, String type) {
        checkState(indexExists(index), "create the index before creating type");
        if (typeExists(index, type)) return;

        PutMappingResponse response = client.admin().indices().putMapping(
           new PutMappingRequest(index).type(type).source(forcedMappingJson(type))).actionGet();
        checkState(response.isAcknowledged(), "could not create type " + type);
        forceRefresh();
    }

    public String getMapping(String index, String type) {
        ClusterState state = client.admin().cluster().prepareState().setIndices(index)
           .execute().actionGet().getState();

        IndexMetaData indexMetaData = state.getMetaData().hasIndex(index) ? state.getMetaData().index(index) :
           state.getMetaData().indices().valuesIt().next();
        MappingMetaData mapping = checkNotNull(indexMetaData).mapping(type);

        return mapping.source().toString();
    }

    public void forceRefresh(String... indices) {
        client.admin().indices().refresh(Requests.refreshRequest(indices)).actionGet();
    }

    public void dropIndex(String... indices) {
        DeleteIndexResponse response = client.admin().indices().delete(new DeleteIndexRequest(indices)
           .indicesOptions(lenientExpandOpen())).actionGet();
        checkState(response.isAcknowledged(), "Could not clear one or more index", Arrays.toString(indices));
    }

    public void waitForYellowStatus() {
        ClusterHealthRequestBuilder healthRequest = client
           .admin()
           .cluster()
           .prepareHealth()
           .setWaitForRelocatingShards(0)
           .setWaitForYellowStatus();
        ClusterHealthResponse clusterHealth = healthRequest.execute().actionGet();
        List<String> validationFailures = clusterHealth.getAllValidationFailures();
        checkState(validationFailures.isEmpty(), "cluster has validation errors " + validationFailures);
    }
}
