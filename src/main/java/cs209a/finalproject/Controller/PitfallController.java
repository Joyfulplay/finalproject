package cs209a.finalproject.Controller;
import cs209a.finalproject.Service.AnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/pitfalls")
@CrossOrigin
public class PitfallController {
    @Autowired private AnalysisService service;
    @GetMapping
    public List<Map<String, Object>> getPitfalls() { return service.getMultithreadingPitfalls(); }
}
