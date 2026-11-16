
package cl.duoc.campuslab.report;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:reportdlttest;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.consumer.group-id=campuslab-report-dlt-test",
        "campuslab.kafka.topics.booking-events=campuslab.test.booking.events",
        "campuslab.kafka.topics.occupancy-events=campuslab.test.occupancy.events"
})
@EmbeddedKafka(
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                "campuslab.test.booking.events",
                "campuslab.test.booking.events.DLT",
                "campuslab.test.occupancy.events",
                "campuslab.test.occupancy.events.DLT"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReportKafkaDltIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    // PRUEBA 1: DLT de reservas
    @Test
    void debeEnviarMensajeInvalidoALaDLT() throws Exception {

        verificarEnvioADLT(
                "campuslab.test.booking.events",
                "{mensaje-invalido-reserva}"
        );
    }

    // PRUEBA 2: DLT de ocupacion
    @Test
    void debeEnviarOcupacionInvalidaALaDLT() throws Exception {

        verificarEnvioADLT(
                "campuslab.test.occupancy.events",
                "{mensaje-invalido-ocupacion}"
        );
    }

    // Metodo reutilizable para verificar ambos topicos.
    private void verificarEnvioADLT(
            String topicoOriginal,
            String mensajeInvalido
    ) throws Exception {

        String topicoDLT = topicoOriginal + ".DLT";

        Map<String, Object> propiedades =
                KafkaTestUtils.consumerProps(
                        "verificador-dlt-" + UUID.randomUUID(),
                        "false",
                        kafkaBroker
                );

        propiedades.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        DefaultKafkaConsumerFactory<String, String> fabrica =
                new DefaultKafkaConsumerFactory<>(
                        propiedades,
                        new StringDeserializer(),
                        new StringDeserializer()
                );

        try (Consumer<String, String> verificador =
                     fabrica.createConsumer()) {

            kafkaBroker.consumeFromAnEmbeddedTopic(
                    verificador,
                    topicoDLT
            );

            kafkaTemplate.send(
                    topicoOriginal,
                    mensajeInvalido
            ).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, String> recibido =
                    KafkaTestUtils.getSingleRecord(
                            verificador,
                            topicoDLT,
                            Duration.ofSeconds(30)
                    );

            assertEquals(
                    mensajeInvalido,
                    recibido.value()
            );

            assertEquals(
                    topicoDLT,
                    recibido.topic()
            );
        }
    }
}
