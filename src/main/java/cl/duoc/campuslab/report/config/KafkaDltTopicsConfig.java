
package cl.duoc.campuslab.report.config;

import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
@ConditionalOnProperty(
        name = "campuslab.kafka.provision-dlt-topics",
        havingValue = "true"
)
public class KafkaDltTopicsConfig {

    // Retencion: 14 dias
    private static final String RETENCION = "1209600000";

    @Bean
    public NewTopic bookingDltTopic(
            @Value("${campuslab.kafka.topics.booking-events}")
            String bookingTopic
    ) {
        return TopicBuilder
                .name(bookingTopic + ".DLT")
                .partitions(1)
                .replicas(1)
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
                .partitions(1)
                .replicas(1)
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
