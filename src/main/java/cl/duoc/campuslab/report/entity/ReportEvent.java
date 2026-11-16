
package cl.duoc.campuslab.report.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(
        name = "report_events",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_report_event_source",
                columnNames = {"event_id", "source_topic"}
        )
)
public class ReportEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "source_topic", nullable = false)
    private String sourceTopic;

    private String eventType;

    private Long bookingId;

    private String resourceId;

    private String status;

    private LocalDateTime occurredAt;

    private LocalDateTime receivedAt;

    public ReportEvent() {
    }

    public ReportEvent(
            String eventId,
            String sourceTopic,
            String eventType,
            Long bookingId,
            String resourceId,
            String status,
            LocalDateTime occurredAt
    ) {
        this.eventId = eventId;
        this.sourceTopic = sourceTopic;
        this.eventType = eventType;
        this.bookingId = bookingId;
        this.resourceId = resourceId;
        this.status = status;
        this.occurredAt = occurredAt;
        this.receivedAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getEventType() {
        return eventType;
    }

    public Long getBookingId() {
        return bookingId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getStatus() {
        return status;
    }

    public LocalDateTime getOccurredAt() {
        return occurredAt;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }
}
