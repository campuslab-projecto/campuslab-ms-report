
package cl.duoc.campuslab.report;

import cl.duoc.campuslab.report.dto.BookingKafkaEvent;
import cl.duoc.campuslab.report.dto.OccupancyKafkaEvent;
import cl.duoc.campuslab.report.repository.ReportEventRepository;
import cl.duoc.campuslab.report.service.ReportService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:reporttest;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false"
})
@Transactional
class ReportServiceIntegrationTest {

    @Autowired
    private ReportService reportService;

    @Autowired
    private ReportEventRepository repository;

    // PRUEBA 1: Guardar eventos y evitar duplicados
    @Test
    void debeGuardarEventosYEvitarDuplicados() {

        LocalDateTime fecha = LocalDateTime.now().minusMinutes(5);

        BookingKafkaEvent reserva = new BookingKafkaEvent(
                "test-booking-001",
                "BOOKING_CREATED",
                101L,
                "SOLICITADA",
                fecha
        );

        OccupancyKafkaEvent ocupacion =
                new OccupancyKafkaEvent(
                        "test-occupancy-001",
                        "OCCUPANCY_CHANGED",
                        101L,
                        "LAB-01",
                        "APROBADA",
                        fecha
                );

        reportService.registrarReserva(reserva);
        reportService.registrarOcupacion(ocupacion);

        // Repetimos ambos eventos
        reportService.registrarReserva(reserva);
        reportService.registrarOcupacion(ocupacion);

        Map<String, Object> kpis =
                reportService.obtenerKpis();

        assertEquals(2L, repository.count());

        assertEquals(2L, kpis.get("totalEventos"));
        assertEquals(1L, kpis.get("reservasDistintas"));
        assertEquals(1L, kpis.get("eventosReservas"));
        assertEquals(1L, kpis.get("eventosOcupacion"));
        assertEquals(1L, kpis.get("recursosDistintos"));
    }

    // PRUEBA 2: Ranking de laboratorios
    @Test
    void debeCalcularRankingDeRecursos() {

        LocalDateTime fecha = LocalDateTime.now().minusMinutes(5);

        reportService.registrarOcupacion(
                new OccupancyKafkaEvent(
                        "ranking-001",
                        "OCCUPANCY_CHANGED",
                        201L,
                        "LAB-01",
                        "APROBADA",
                        fecha
                )
        );

        reportService.registrarOcupacion(
                new OccupancyKafkaEvent(
                        "ranking-002",
                        "OCCUPANCY_CHANGED",
                        202L,
                        "LAB-01",
                        "APROBADA",
                        fecha
                )
        );

        reportService.registrarOcupacion(
                new OccupancyKafkaEvent(
                        "ranking-003",
                        "OCCUPANCY_CHANGED",
                        203L,
                        "LAB-02",
                        "APROBADA",
                        fecha
                )
        );

        // Otro evento de la misma reserva:
        // no debe contarla dos veces en el ranking.
        reportService.registrarOcupacion(
                new OccupancyKafkaEvent(
                        "ranking-004",
                        "OCCUPANCY_CHANGED",
                        201L,
                        "LAB-01",
                        "EN_USO",
                        fecha
                )
        );

        List<Map<String, Object>> ranking =
                reportService.obtenerTopRecursos();

        // Deben aparecer dos laboratorios
        assertEquals(2, ranking.size());

        // LAB-01 tiene dos reservas distintas
        assertEquals("LAB-01", ranking.get(0).get("resourceId"));

        assertEquals(
                2L,
                ((Number) ranking.get(0)
                        .get("totalReservas")).longValue()
        );

        // LAB-02 tiene una reserva
        assertEquals("LAB-02", ranking.get(1).get("resourceId"));

        assertEquals(
                1L,
                ((Number) ranking.get(1)
                        .get("totalReservas")).longValue()
        );
    }
}
