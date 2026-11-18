
package cl.duoc.campuslab.report.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConditionalOnProperty(
        name = "campuslab.kafka.provision-dlt-topics",
        havingValue = "true"
)
public class KafkaDltTopicsConfig {

    // Retencion de mensajes: 14 dias.
    private static final String RETENCION = "1209600000";

    // Se requiere un minimo de dos replicas sincronizadas.
    private static final String MIN_ISR = "2";

    @Bean
    public NewTopic bookingDltTopic(
            @Value("${campuslab.kafka.topics.booking-events}")
            String bookingTopic
    ) {
        return TopicBuilder
                .name(bookingTopic + ".DLT")
                .partitions(3)
                .replicas(3)
                .config(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        MIN_ISR
                )
                .config(
                        TopicConfig.RETENTION_MS_CONFIG,
                        RETENCION
                )
                .config(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_DELETE
                )
                .build();
    }

    @Bean
    public NewTopic occupancyDltTopic(
            @Value("${campuslab.kafka.topics.occupancy-events}")
            String occupancyTopic
    ) {
        return TopicBuilder
                .name(occupancyTopic + ".DLT")
                .partitions(3)
                .replicas(3)
                .config(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        MIN_ISR
                )
                .config(
                        TopicConfig.RETENTION_MS_CONFIG,
                        RETENCION
                )
                .config(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_DELETE
                )
                .build();
    }
}
