
package cl.duoc.campuslab.report.service;

import cl.duoc.campuslab.report.dto.BookingKafkaEvent;
import cl.duoc.campuslab.report.dto.OccupancyKafkaEvent;
import cl.duoc.campuslab.report.entity.ReportEvent;
import cl.duoc.campuslab.report.repository.ReportEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;

@Service
public class ReportService {

    private static final Logger log =
            LoggerFactory.getLogger(ReportService.class);

    private final ReportEventRepository repository;

    private final String bookingTopic;
    private final String occupancyTopic;

    public ReportService(
            ReportEventRepository repository,
            @Value("${campuslab.kafka.topics.booking-events}")
            String bookingTopic,
            @Value("${campuslab.kafka.topics.occupancy-events}")
            String occupancyTopic
    ) {
        this.repository = repository;
        this.bookingTopic = bookingTopic;
        this.occupancyTopic = occupancyTopic;
    }

    // Guarda eventos relacionados con reservas
    @Transactional
    public void registrarReserva(BookingKafkaEvent evento) {

        if (evento == null ||
                evento.eventId() == null ||
                evento.eventId().isBlank() ||
                evento.bookingId() == null ||
                evento.eventType() == null ||
                evento.eventType().isBlank()) {

            throw new IllegalArgumentException(
                    "Evento de reserva incompleto"
            );
        }

        if (repository.existsByEventIdAndSourceTopic(
                evento.eventId(), bookingTopic)) {

            log.info(
                    "Evento de reserva duplicado: {}",
                    evento.eventId()
            );

            return;
        }

        ReportEvent registro = new ReportEvent(
                evento.eventId(),
                bookingTopic,
                evento.eventType(),
                evento.bookingId(),
                null,
                evento.status(),
                evento.timestamp()
        );

        repository.saveAndFlush(registro);

        log.info(
                "Evento de reserva almacenado: eventId={}, bookingId={}",
                evento.eventId(),
                evento.bookingId()
        );
    }

    // Guarda eventos relacionados con ocupacion
    @Transactional
    public void registrarOcupacion(OccupancyKafkaEvent evento) {

        if (evento == null ||
                evento.eventId() == null ||
                evento.eventId().isBlank() ||
                evento.bookingId() == null ||
                evento.eventType() == null ||
                evento.eventType().isBlank() ||
                evento.resourceId() == null ||
                evento.resourceId().isBlank()) {

            throw new IllegalArgumentException(
                    "Evento de ocupacion incompleto"
            );
        }

        if (repository.existsByEventIdAndSourceTopic(
                evento.eventId(), occupancyTopic)) {

            log.info(
                    "Evento de ocupacion duplicado: {}",
                    evento.eventId()
            );

            return;
        }

        ReportEvent registro = new ReportEvent(
                evento.eventId(),
                occupancyTopic,
                evento.eventType(),
                evento.bookingId(),
                evento.resourceId(),
                evento.status(),
                evento.timestamp()
        );

        repository.saveAndFlush(registro);

        log.info(
                "Evento de ocupacion almacenado: eventId={}, resourceId={}",
                evento.eventId(),
                evento.resourceId()
        );
    }

    // Calcula indicadores generales de reporteria
    @Transactional(readOnly = true)
    public Map<String, Object> obtenerKpis() {

        List<ReportEvent> eventos = repository.findAll();

        long totalEventos = eventos.size();

        long reservasDistintas = eventos.stream()
                .map(ReportEvent::getBookingId)
                .filter(Objects::nonNull)
                .distinct()
                .count();

        long eventosReservas = eventos.stream()
                .filter(e ->
                        bookingTopic.equals(e.getSourceTopic()))
                .count();

        long eventosOcupacion = eventos.stream()
                .filter(e ->
                        occupancyTopic.equals(e.getSourceTopic()))
                .count();

        long recursosDistintos = eventos.stream()
                .map(ReportEvent::getResourceId)
                .filter(id ->
                        id != null && !id.isBlank())
                .distinct()
                .count();

        Map<String, Object> kpis = new LinkedHashMap<>();

        kpis.put("totalEventos", totalEventos);
        kpis.put("reservasDistintas", reservasDistintas);
        kpis.put("eventosReservas", eventosReservas);
        kpis.put("eventosOcupacion", eventosOcupacion);
        kpis.put("recursosDistintos", recursosDistintos);

        return kpis;
    }

    // Obtiene los recursos con mas reservas
    // durante los ultimos 7 dias
    @Transactional(readOnly = true)
    public List<Map<String, Object>> obtenerTopRecursos() {

        LocalDateTime desde =
                LocalDateTime.now().minusDays(7);

        List<ReportEventRepository.ResourceUsage> resultados =
                repository.findTopResources(
                        occupancyTopic,
                        desde
                );

        return resultados.stream()
                .map(recurso -> {

                    Map<String, Object> item =
                            new LinkedHashMap<>();

                    item.put(
                            "resourceId",
                            recurso.getResourceId()
                    );

                    item.put(
                            "totalReservas",
                            recurso.getTotalReservas()
                    );

                    return item;
                })
                .toList();
    }
}
