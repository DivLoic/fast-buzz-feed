package com.pubsap.eng;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

import static org.junit.Assert.fail;

/**
 * Created by loicmdivad.
 */
public class BaseCaseIt {

    public static SchemaRegistryClient client;

    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File(TestProvider.getPathFromResources("docker-compose-it.yml")))
                    .withExposedService("schema-registry_1", 8081, Wait.forHttp("/subjects").forStatusCode(200));


    @BeforeAll
    public static void setUp() {
    }

    @AfterAll
    public static void tearDown() {
    }

    @Test
    public void applicationShouldDoSomething() {
        fail("Testing the proper execution of integration tests!");
    }
}
