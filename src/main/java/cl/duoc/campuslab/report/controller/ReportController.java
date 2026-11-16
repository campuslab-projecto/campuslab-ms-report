
package cl.duoc.campuslab.report.controller;

import cl.duoc.campuslab.report.service.ReportService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/report")
public class ReportController {

    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    // Consulta los indicadores generales de CampusLab
    @GetMapping("/kpis")
    public ResponseEntity<Map<String, Object>> obtenerKpis() {

        Map<String, Object> indicadores =
                reportService.obtenerKpis();

        return ResponseEntity.ok(indicadores);
    }

    // Consulta los recursos con mas reservas
    // durante los ultimos siete dias
    @GetMapping("/top-resources")
    public ResponseEntity<List<Map<String, Object>>> obtenerTopRecursos() {

        List<Map<String, Object>> recursos =
                reportService.obtenerTopRecursos();

        return ResponseEntity.ok(recursos);
    }
}
