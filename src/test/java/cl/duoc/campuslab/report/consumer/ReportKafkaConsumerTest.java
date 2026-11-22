
package cl.duoc.campuslab.report.consumer;

import cl.duoc.campuslab.report.service.ReportService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReportKafkaConsumerTest {

    @Mock
    private ReportService reportService;

    private ReportKafkaConsumer consumer;

    @BeforeEach
    void preparar() {

        ObjectMapper objectMapper =
                new ObjectMapper()
                        .registerModule(new JavaTimeModule());

        consumer = new ReportKafkaConsumer(
                objectMapper,
                reportService
        );
    }

    // PRUEBA 1: Interpretar un evento de reserva
    @Test
    void debeProcesarEventoDeReserva() {

        String mensaje = """
                {
                  "eventId": "booking-test-001",
                  "eventType": "BOOKING_CREATED",
                  "bookingId": 101,
                  "status": "SOLICITADA",
                  "timestamp": "2026-11-16T10:00:00"
                }
                """;

        consumer.consumirReserva(mensaje);

        verify(reportService).registrarReserva(
                argThat(evento ->
                        "booking-test-001".equals(evento.eventId())
                                && evento.bookingId() == 101L
                                && evento.resourceId() == null
                                && "BOOKING_CREATED".equals(
                                evento.eventType()
                        )
                )
        );
    }

    // PRUEBA 2: Interpretar un evento de ocupacion
    @Test
    void debeProcesarEventoDeOcupacion() {

        String mensaje = """
                {
                  "eventId": "occupancy-test-001",
                  "eventType": "OCCUPANCY_CHANGED",
                  "bookingId": 102,
                  "resourceId": "LAB-01",
                  "status": "APROBADA",
                  "timestamp": "2026-11-16T10:30:00"
                }
                """;

        consumer.consumirOcupacion(mensaje);

        verify(reportService).registrarOcupacion(
                argThat(evento ->
                        "occupancy-test-001".equals(evento.eventId())
                                && evento.bookingId() == 102L
                                && "LAB-01".equals(evento.resourceId())
                )
        );
    }

    // PRUEBA 3: Rechazar un mensaje JSON invalido
    @Test
    void debeRechazarJsonInvalido() {

        String mensaje = "{mensaje-invalido}";

        assertThrows(
                IllegalArgumentException.class,
                () -> consumer.consumirReserva(mensaje)
        );

        verifyNoInteractions(reportService);
    }

    // Verifica el nuevo formato enviado por Bookings EP6
    @Test
    void debeProcesarReservaConResourceId() {

        String mensaje = """
                {
                  "eventId": "booking-ep6-001",
                  "eventType": "BOOKING_CREATED",
                  "bookingId": 301,
                  "resourceId": "LAB-EP6-TEST-01",
                  "status": "SOLICITADA",
                  "timestamp": "2026-11-22T10:00:00"
                }
                """;

        consumer.consumirReserva(mensaje);

        verify(reportService).registrarReserva(
                argThat(evento ->
                        "booking-ep6-001".equals(evento.eventId())
                                && Long.valueOf(301L).equals(evento.bookingId())
                                && "LAB-EP6-TEST-01".equals(evento.resourceId())
                )
        );
    }
}
