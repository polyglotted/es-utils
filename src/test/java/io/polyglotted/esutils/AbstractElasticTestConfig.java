package io.polyglotted.esutils;

import applauncher.spring.ElasticConfiguration;
import com.typesafe.config.ConfigFactory;
import io.polyglotted.applauncher.settings.DefaultSettingsHolder;
import io.polyglotted.applauncher.settings.SettingsHolder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        ElasticConfiguration.class,
})
public class AbstractElasticTestConfig {

    @Bean
    public SettingsHolder settingsHolder() {
        return new DefaultSettingsHolder(ConfigFactory.load("unittest"));
    }
}
