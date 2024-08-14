package org.tekeli.borisp.kafkagames

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.containers.KafkaContainer
import java.time.Duration


private const val topic = "input"

@SpringBootTest(classes = [TestcontainersConfiguration::class])
class ReadWrittenTest {

    @Autowired
    lateinit var kafkaContainer: KafkaContainer

    @BeforeEach
    fun setup() {
        kafkaContainer.start()
    }

    @Test
    fun shouldReadAWrittenMessage() {
        val producer = producer()
        val consumer = consumer()
        consumer.subscribe(listOf(topic))

        producer.send(topic, "Hello, World!")

        val records = consumer.poll(Duration.ofSeconds(10))
        assertThat(records).isNotEmpty
        val firstOrNull = records.firstOrNull { it.value() == "Hello, World!" }
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