package com.pubsap.eng.streams;

import com.jasongoodwin.monads.Try;
import com.pubsap.eng.ITProvider;
import com.pubsap.eng.schema.Input;
import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Output;
import com.pubsap.eng.schema.OutputKey;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static com.pubsap.eng.ITProvider.*;
import static com.pubsap.eng.TestProvider.fileFromResource;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Created by loicmdivad.
 */
public class FizzBuzzIt {

    public static Config config;

    public static SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>();
    public static SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>();
    public static SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>();
    public static SpecificAvroSerde<OutputKey> outputKeySerde = new SpecificAvroSerde<>();

    public static KafkaProducer<InputKey, Input> producer;
    public static KafkaConsumer<OutputKey, Output> consumer;

    private static Process application;

    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(fileFromResource("docker-compose-it.yml"))
                    .withExposedService("broker", 9092,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
                    .withExposedService("schema-registry", 8081,
                            Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

    @BeforeAll
    public static void setUpAll() throws IOException {
        environment.start();

        System.setProperty("config.file", fileFromResource("application-it.conf").getPath());
        System.setProperty("logback.configurationFile", fileFromResource("logback-it.xml").getPath());

        config = ConfigFactory.load();

        Map<String, String> schemaRegistryConfigMap =
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, config.getString(SCHEMA_REGISTRY_URL_CONFIG));

        inputSerde.configure(schemaRegistryConfigMap, false);
        outputSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);
        outputKeySerde.configure(schemaRegistryConfigMap, true);

        Map<String, Object> producerConf = new HashMap<String, Object>() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
            put(ProducerConfig.ACKS_CONFIG, "1");
        }};

        Map<String, Object> consumerConf = new HashMap<String, Object>() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
            put(ConsumerConfig.GROUP_ID_CONFIG, "IT-CONSUMER-GRP");
        }};

        topicCreation(
                Collections.singletonMap(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers")),
                Arrays.asList(config.getString("topic.input.name"), config.getString("topic.output.name"))
        );

        producer = new KafkaProducer<>(producerConf, inputKeySerde.serializer(), inputSerde.serializer());
        consumer = new KafkaConsumer<>(consumerConf, outputKeySerde.deserializer(), outputSerde.deserializer());

        consumer.assign(topicPartitions(consumer, config.getString("topic.output.name")));

        List<String> arguments = Collections.emptyList();
        application = ITProvider.exec(Main.class, forwardProperties(), arguments);
    }

    @AfterAll
    public static void tearDownAll() throws InterruptedException {
        application.destroy();
        application.waitFor();

        producer.close();
        consumer.close();
        environment.stop();
    }

    @Test
    public void topologyShouldGroupEvents() {
        //Given
        Stream<ProducerRecord<InputKey, Input>> inputs = Stream.of(
                new Input(3, Instant.parse("2020-02-14T14:26:00Z")),
                new Input(5, Instant.parse("2020-02-14T14:26:01Z")),
                new Input(15, Instant.parse("2020-02-14T14:26:02Z")),
                new Input(3, Instant.parse("2020-02-14T14:26:03Z")),
                new Input(5, Instant.parse("2020-02-14T14:26:04Z")),
                new Input(15, Instant.parse("2020-02-14T14:26:05Z"))

        ).map((value) -> new ProducerRecord<>(config.getString("topic.input.name"), new InputKey("client-1"), value));

        // when
        inputs.forEach((record) -> {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        // Then
        Supplier<Output> parseResult = () -> Try.ofFailable(() -> {
            Thread.sleep(300);
            consumer.seekToBeginning(topicPartitions(consumer, config.getString("topic.output.name")));
            Iterable<ConsumerRecord<OutputKey, Output>> it = () -> consumer.poll(Duration.ofSeconds(3)).iterator();

            return StreamSupport.stream(it.spliterator(), false)
                    .filter((record) -> record.key().getName().equals("client-1"))
                    .sorted((o1, o2) -> (int) (o1.timestamp() - o2.timestamp()))
                    .reduce((previous, last) -> last)
                    .map(ConsumerRecord::value)
                    .get();

        }).orElse(Output.newBuilder().build());

        Assert.assertThat(parseResult, eventuallyEval(is(new Output(2, 2, 2)), Duration.ofSeconds(30)));
    }

    @Test
    public void topologyShouldNotGroupEventsFromSameWindow() {
        //Given
        Stream<ProducerRecord<InputKey, Input>> inputs = Stream.of(
                new Input(3, Instant.parse("2020-02-14T14:26:05Z")),
                new Input(5, Instant.parse("2020-02-14T14:26:25Z")),
                new Input(15, Instant.parse("2020-02-14T14:26:45Z"))

        ).map((value) -> new ProducerRecord<>(config.getString("topic.input.name"), new InputKey("client-2"), value));

        // when
        inputs.forEach((record) -> {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        // Then
        Supplier<List<Output>> parseResult = () -> Try.ofFailable(() -> {
            Thread.sleep(300);
            consumer.seekToBeginning(topicPartitions(consumer, config.getString("topic.output.name")));
            Iterable<ConsumerRecord<OutputKey, Output>> it = () -> consumer.poll(Duration.ofSeconds(3)).iterator();

            return StreamSupport.stream(it.spliterator(), false)
                    .filter((record) -> record.key().getName().equals("client-2"))
                    .sorted((o1, o2) -> (int) (o1.timestamp() - o2.timestamp()))
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());

        }).orElse(Collections.emptyList());

        Output[] expected = {
                new Output(1, 0, 0),
                new Output(0, 1, 0),
                new Output(0, 0, 1)
        };

        Assert.assertThat(() -> parseResult.get().size(), eventuallyEval(equalTo(3), Duration.ofSeconds(40)));

        Assert.assertThat(parseResult.get(), containsInAnyOrder(expected));
    }
}
