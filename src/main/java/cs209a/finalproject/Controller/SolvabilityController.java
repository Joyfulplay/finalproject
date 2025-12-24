package cs209a.finalproject.Controller;

import cs209a.finalproject.Service.AnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/solvability")
@CrossOrigin
public class SolvabilityController {
    @Autowired private AnalysisService service;
    @GetMapping
    public List<Map<String, Object>> getSolvability() {
        return service.getSolvabilityComparison();
    }
}