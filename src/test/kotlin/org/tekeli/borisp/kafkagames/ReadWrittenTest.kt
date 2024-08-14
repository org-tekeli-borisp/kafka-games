package org.tekeli.borisp.kafkagames

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration

@SpringBootTest
class ReadWrittenTest {

    @Container
    private var kafkaContainer: KafkaContainer =
        KafkaContainer(DockerImageName.parse("apache/kafka-native:latest").asCompatibleSubstituteFor("apache/kafka"))

    @Value("\${spring.kafka.topics.greeting}")
    lateinit var topic: String

    lateinit var producer: KafkaTemplate<String, String>
    lateinit var consumer: Consumer<String, String>

    @BeforeEach
    fun setup() {
        kafkaContainer.start()
        producer = producer()
        consumer = consumer()
        consumer.subscribe(listOf(topic))
    }

    @AfterEach
    fun tearDown() {
        consumer.close(Duration.ofSeconds(10))
    }

    @Test
    fun shouldReadAWrittenMessage() {
        val message = "Hello, World!"

        producer.send(topic, message)

        val records = consumer.poll(Duration.ofSeconds(10))
        assertThat(records).isNotEmpty
        val firstOrNull = records.firstOrNull { it.value() == message }
        assertThat(firstOrNull).isNotNull
    }

    private fun producer(): KafkaTemplate<String, String> {
        val producerFactory = DefaultKafkaProducerFactory<String, String>(
            mutableMapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to this.kafkaContainer.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        )
        val kafkaTemplate = KafkaTemplate(producerFactory)
        return kafkaTemplate
    }


    private fun consumer(): Consumer<String, String> {
        val consumerFactory = DefaultKafkaConsumerFactory<String, String>(
            mutableMapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to this.kafkaContainer.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "consumerGroupId",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )
        return consumerFactory.createConsumer()
    }
}