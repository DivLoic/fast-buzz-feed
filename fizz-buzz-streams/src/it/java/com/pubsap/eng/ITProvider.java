package com.pubsap.eng;

import com.jasongoodwin.monads.Try;
import com.pubsap.eng.schema.Output;
import com.pubsap.eng.schema.OutputKey;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Created by loicmdivad.
 */
public class ITProvider {

    private static final Logger logger = LoggerFactory.getLogger(ITProvider.class);

    public static List<TopicPartition> topicPartitions(KafkaConsumer<OutputKey, Output> consumer, String topic) {

        return  consumer
                .partitionsFor(topic)
                .stream()
                .map((info) -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
    }

    public static void topicCreation(Map<String, Object> properties, List<String> topicNames) {
        Admin client = Admin.create(properties);
        CreateTopicsResult result = client.createTopics(
                topicNames
                        .stream()
                        .map((name) -> new NewTopic(name, 1, Short.decode("1")))
                        .collect(Collectors.toList())
        );

        try {
            result.all().get();
            logger.info("Kafka Topics are created");
            client.close();
            logger.info("Kafka Admin Client is closed");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static List<String> forwardProperties() {
        return System
                .getProperties()
                .entrySet()
                .stream()
                .map((entry) ->
                        String.format("-D%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.toList()
                );
    }

    public static Process exec(Class clazz, List<String> jvmArgs, List<String> args) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = clazz.getName();

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.addAll(jvmArgs);
        command.add("-cp");
        command.add(classpath);
        command.add(className);
        command.addAll(args);

        ProcessBuilder builder = new ProcessBuilder(command);

        Process process = builder.start();
        new Thread(() -> Try.ofFailable(() -> IOUtils.copy(process.getInputStream(), System.out))).start();
        new Thread(() -> Try.ofFailable(() -> IOUtils.copy(process.getErrorStream(), System.err))).start();

        return process;
    }
}
