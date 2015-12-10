package applauncher.spring;

import fr.pilato.spring.elasticsearch.ElasticsearchClientFactoryBean;
import fr.pilato.spring.elasticsearch.ElasticsearchNodeFactoryBean;
import io.polyglotted.applauncher.settings.SettingsHolder;
import io.polyglotted.eswrapper.services.AdminWrapper;
import io.polyglotted.eswrapper.services.IndexerWrapper;
import io.polyglotted.eswrapper.services.QueryWrapper;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@Configuration
public class ElasticConfiguration {

    @Autowired @Qualifier("settingsHolder")
    private final SettingsHolder settingsHolder = null;

    @Bean(name = "es-admin")
    public AdminWrapper elasticSearchAdmin() throws Exception {
        return new AdminWrapper(client());
    }

    @Bean(name = "es-query")
    public QueryWrapper elasticSearchQuery() throws Exception {
        return new QueryWrapper(client());
    }

    @Bean(name = "es-indexer")
    public IndexerWrapper elasticSearchIndexer() throws Exception {
        return new IndexerWrapper(client());
    }

    @Bean
    public Client client() throws Exception {
        return elasticsearchClientFactoryBean().getObject();
    }

    @Bean
    ElasticsearchClientFactoryBean elasticsearchClientFactoryBean() throws Exception {
        ElasticsearchClientFactoryBean elasticsearchClientFactoryBean = new ElasticsearchClientFactoryBean();
        elasticsearchClientFactoryBean.setNode(node());
        elasticsearchClientFactoryBean.setAutoscan(true);
        elasticsearchClientFactoryBean.setMergeMapping(false);
        elasticsearchClientFactoryBean.setMergeSettings(true);
        return elasticsearchClientFactoryBean;
    }

    @Bean
    public Node node() throws Exception {
        return nodeFactoryBean().getObject();
    }

    @Bean
    ElasticsearchNodeFactoryBean nodeFactoryBean() {
        ElasticsearchNodeFactoryBean factoryBean = new ElasticsearchNodeFactoryBean();
        factoryBean.setProperties(elasticProperties());
        return factoryBean;
    }

    @Bean
    Properties elasticProperties() {
        return checkNotNull(settingsHolder).asProperties("es", false);
    }
}