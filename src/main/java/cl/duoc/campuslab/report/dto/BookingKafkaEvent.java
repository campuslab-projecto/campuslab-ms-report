
package cl.duoc.campuslab.report.dto;

import java.time.LocalDateTime;

public record BookingKafkaEvent(
        String eventId,
        String eventType,
        Long bookingId,
        String status,
        LocalDateTime timestamp
) {
}
