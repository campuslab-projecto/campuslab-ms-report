
package cl.duoc.campuslab.report.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate
    ) {

        // Define el destino de los mensajes fallidos.
        DeadLetterPublishingRecoverer recuperador =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (registro, excepcion) ->
                                new TopicPartition(
                                        registro.topic() + ".DLT",
                                        registro.partition()
                                )
                );

        // Si falla el envio a la DLT, no considerarlo recuperado.
        recuperador.setFailIfSendResultIsError(true);

        // Un intento inicial y dos reintentos.
        FixedBackOff reintentos =
                new FixedBackOff(1000L, 2L);

        return new DefaultErrorHandler(
                recuperador,
                reintentos
        );
    }
}
