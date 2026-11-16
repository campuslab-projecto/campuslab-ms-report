
package cl.duoc.campuslab.report.dto;

import java.time.LocalDateTime;

public record OccupancyKafkaEvent(
        String eventId,
        String eventType,
        Long bookingId,
        String resourceId,
        String status,
        LocalDateTime timestamp
) {
}
