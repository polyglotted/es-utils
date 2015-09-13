package applauncher.spring;

import fr.pilato.spring.elasticsearch.ElasticsearchClientFactoryBean;
import fr.pilato.spring.elasticsearch.ElasticsearchNodeFactoryBean;
import io.polyglotted.applauncher.settings.SettingsHolder;
import io.polyglotted.esutils.services.AdminWrapper;
import io.polyglotted.esutils.services.QueryWrapper;
import lombok.SneakyThrows;
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

    @Bean
    public AdminWrapper admin() {
        return new AdminWrapper(client());
    }

    @Bean
    public QueryWrapper query() {
        return new QueryWrapper(client());
    }

    @Bean
    @SneakyThrows
    public Client client() {
        return elasticsearchClientFactoryBean().getObject();
    }

    @Bean
    ElasticsearchClientFactoryBean elasticsearchClientFactoryBean() {
        ElasticsearchClientFactoryBean elasticsearchClientFactoryBean = new ElasticsearchClientFactoryBean();
        elasticsearchClientFactoryBean.setNode(node());
        elasticsearchClientFactoryBean.setAutoscan(true);
        elasticsearchClientFactoryBean.setMergeMapping(false);
        elasticsearchClientFactoryBean.setMergeSettings(true);
        return elasticsearchClientFactoryBean;
    }

    @Bean
    @SneakyThrows
    public Node node() {
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