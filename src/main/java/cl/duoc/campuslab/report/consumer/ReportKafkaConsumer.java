
package cl.duoc.campuslab.report.consumer;

import cl.duoc.campuslab.report.dto.BookingKafkaEvent;
import cl.duoc.campuslab.report.dto.OccupancyKafkaEvent;
import cl.duoc.campuslab.report.service.ReportService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReportKafkaConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(ReportKafkaConsumer.class);

    private final ObjectMapper objectMapper;
    private final ReportService reportService;

    public ReportKafkaConsumer(
            ObjectMapper objectMapper,
            ReportService reportService
    ) {
        this.objectMapper = objectMapper;
        this.reportService = reportService;
    }

    // Consume eventos del topico de reservas
    @KafkaListener(
            topics = "${campuslab.kafka.topics.booking-events}"
    )
    public void consumirReserva(String mensaje) {

        try {
            BookingKafkaEvent evento =
                    objectMapper.readValue(
                            mensaje,
                            BookingKafkaEvent.class
                    );

            reportService.registrarReserva(evento);

            log.info(
                    "Evento de reserva procesado: bookingId={}, eventId={}",
                    evento.bookingId(),
                    evento.eventId()
            );

        } catch (JsonProcessingException e) {

            log.error(
                    "JSON invalido en evento de reserva",
                    e
            );

            throw new IllegalArgumentException(
                    "No se pudo interpretar el evento de reserva",
                    e
            );
        }
    }

    // Consume eventos del topico de ocupacion
    @KafkaListener(
            topics = "${campuslab.kafka.topics.occupancy-events}"
    )
    public void consumirOcupacion(String mensaje) {

        try {
            OccupancyKafkaEvent evento =
                    objectMapper.readValue(
                            mensaje,
                            OccupancyKafkaEvent.class
                    );

            reportService.registrarOcupacion(evento);

            log.info(
                    "Evento de ocupacion procesado: resourceId={}, eventId={}",
                    evento.resourceId(),
                    evento.eventId()
            );

        } catch (JsonProcessingException e) {

            log.error(
                    "JSON invalido en evento de ocupacion",
                    e
            );

            throw new IllegalArgumentException(
                    "No se pudo interpretar el evento de ocupacion",
                    e
            );
        }
    }
}
