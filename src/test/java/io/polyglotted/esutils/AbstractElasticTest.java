package io.polyglotted.esutils;

import io.polyglotted.esutils.services.AdminWrapper;
import io.polyglotted.esutils.services.QueryWrapper;
import lombok.SneakyThrows;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import java.io.File;

import static org.elasticsearch.common.io.FileSystemUtils.deleteRecursively;

@ContextConfiguration(classes = AbstractElasticTestConfig.class)
public abstract class AbstractElasticTest extends AbstractTestNGSpringContextTests {

    @Autowired
    protected final Client client = null;

    @Autowired
    protected final AdminWrapper admin = null;

    @Autowired
    protected final QueryWrapper query = null;

    @BeforeSuite
    public void cleanES() {
        deleteRecursively(new File("elastic-test"), true);
    }

    @BeforeMethod
    @SneakyThrows
    public void setUp() throws Exception {
        performSetup();
        Thread.sleep(50);
    }

    protected void performSetup() {
        //sub-classes can extend
    }
}
