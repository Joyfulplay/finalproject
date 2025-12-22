package cs209a.finalproject.Controller;

import cs209a.finalproject.Service.AnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/co-occurrence")
@CrossOrigin
public class CoOccurrenceController {
    @Autowired private AnalysisService service;
    @GetMapping
    public List<Map<String, Object>> getPairs(@RequestParam(defaultValue = "15") int n) {
        return service.getCoOccurrence(n);
    }
}
