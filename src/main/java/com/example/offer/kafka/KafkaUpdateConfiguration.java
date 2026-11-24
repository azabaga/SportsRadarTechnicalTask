package com.example.offer.kafka;

import com.example.offer.application.InMemoryOfferStore;
import com.example.offer.json.OfferJson;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

@Configuration(proxyBeanMethods = false)
@EnableKafka
@Profile("kafka")
class KafkaUpdateConfiguration {
    static final String MARKET_TOPIC = "market";
    static final String EVENT_TOPIC = "event";
    static final int EVENT_PARTITIONS = 2;

    @Bean
    Map<String, Object> offerConsumerProperties(
            @Value("${offer.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${offer.kafka.group-id}") String groupId
    ) {
        return Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> offerKafkaListenerContainerFactory(
            @Qualifier("offerConsumerProperties") Map<String, Object> consumerProperties) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProperties));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setSyncCommits(true);
        factory.getContainerProperties().setMissingTopicsFatal(true);
        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
        return factory;
    }

    @Bean
    KafkaOfferUpdates kafkaOfferUpdates(InMemoryOfferStore store, OfferJson json,
                                       @Qualifier("offerConsumerProperties") Map<String, Object> consumerProperties,
                                       @Value("${offer.kafka.bootstrap-servers}") String bootstrapServers) {
        var admin = new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name()
        ));
        admin.setOperationTimeout(10);
        admin.createOrModifyTopics(
                new NewTopic(MARKET_TOPIC, 1, (short) 1),
                new NewTopic(EVENT_TOPIC, EVENT_PARTITIONS, (short) 1)
        );
        var topics = admin.describeTopics(MARKET_TOPIC, EVENT_TOPIC);
        if (topics.get(MARKET_TOPIC).partitions().size() != 1
                || topics.get(EVENT_TOPIC).partitions().size() < EVENT_PARTITIONS) {
            throw new IllegalStateException("Kafka requires one market partition and at least two event partitions");
        }
        var updates = new KafkaOfferUpdates(store, json);
        var partitions = topics.values().stream()
                .flatMap(topic -> topic.partitions().stream()
                        .map(partition -> new TopicPartition(topic.name(), partition.partition())))
                .toList();
        try (var consumer = new KafkaConsumer<String, String>(consumerProperties)) {
            KafkaReplay.restore(consumer, partitions, updates);
        }
        return updates;
    }
}
