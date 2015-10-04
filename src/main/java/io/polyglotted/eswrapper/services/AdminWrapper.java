package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.indexing.Alias;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.TypeMapping;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.indexAliasesRequest;
import static org.elasticsearch.client.Requests.updateSettingsRequest;

@RequiredArgsConstructor
public final class AdminWrapper {
    private final Client client;

    public boolean indexExists(String... indices) {
        IndicesAdminClient indicesAdmin = client.admin().indices();
        return indicesAdmin.exists(new IndicesExistsRequest(indices)).actionGet().isExists();
    }

    public void createIndex(IndexSetting setting, String... indices) {
        createIndex(setting, ImmutableList.of(), indices);
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

    public void updateSetting(IndexSetting setting, String... indices) {
        checkState(indexExists(indices), "one or more index does not exist " + Arrays.toString(indices));
        UpdateSettingsRequest settingsRequest = updateSettingsRequest(indices).settings(setting.updateJson());
        AcknowledgedResponse response = client.admin().indices().updateSettings(settingsRequest).actionGet();
        checkState(response.isAcknowledged(), "unable to update settings for " + Arrays.toString(indices));
    }

    public void updateAliases(Alias... aliases) {
        IndicesAliasesRequest aliasesRequest = indexAliasesRequest();
        for (Alias alias : aliases) {
            aliasesRequest.addAliasAction(alias.action());
        }
        AcknowledgedResponse response = client.admin().indices().aliases(aliasesRequest).actionGet();
        checkState(response.isAcknowledged(), "unable to update aliases");
    }

    public boolean typeExists(String index, String... types) {
        IndicesAdminClient indicesAdmin = client.admin().indices();
        TypesExistsRequest request = new TypesExistsRequest(new String[]{index}, types);
        return indicesAdmin.typesExists(request).actionGet().isExists();
    }

    public void createType(TypeMapping mapping) {
        checkState(indexExists(mapping.index), "create the index before creating type");
        if (typeExists(mapping.index, mapping.type)) return;

        PutMappingResponse response = client.admin().indices().putMapping(
           new PutMappingRequest(mapping.index).type(mapping.type).source(mapping.mappingJson())).actionGet();
        checkState(response.isAcknowledged(), "could not create type " + mapping.type);
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
           .indicesOptions(IndicesOptions.lenientExpandOpen())).actionGet();
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
