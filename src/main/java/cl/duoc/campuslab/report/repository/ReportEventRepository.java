
package cl.duoc.campuslab.report.repository;

import cl.duoc.campuslab.report.entity.ReportEvent;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface ReportEventRepository
        extends JpaRepository<ReportEvent, Long> {

    // Evita registrar dos veces el mismo evento
    boolean existsByEventIdAndSourceTopic(
            String eventId,
            String sourceTopic
    );

    // Consulta eventos de un topico
    List<ReportEvent> findBySourceTopicOrderByOccurredAtDesc(
            String sourceTopic
    );

    // Cuenta eventos por topico
    long countBySourceTopic(String sourceTopic);

    // Consulta eventos de una reserva
    List<ReportEvent> findByBookingId(Long bookingId);

    // Proyeccion de utilizacion por recurso
    interface ResourceUsage {
        String getResourceId();
        Long getTotalReservas();
    }

    // Ranking de recursos segun reservas distintas
    @Query("""
            SELECT e.resourceId AS resourceId,
                   COUNT(DISTINCT e.bookingId) AS totalReservas
            FROM ReportEvent e
            WHERE e.sourceTopic = :topic
              AND e.resourceId IS NOT NULL
              AND e.occurredAt >= :desde
            GROUP BY e.resourceId
            ORDER BY COUNT(DISTINCT e.bookingId) DESC,
                     e.resourceId ASC
            """)
    List<ResourceUsage> findTopResources(
            @Param("topic") String topic,
            @Param("desde") LocalDateTime desde
    );
}
