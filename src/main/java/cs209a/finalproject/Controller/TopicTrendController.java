package cs209a.finalproject.Controller;

import cs209a.finalproject.Service.AnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/trends")
@CrossOrigin
public class TopicTrendController {
    @Autowired private AnalysisService service;
    @GetMapping
    public List<Map<String, Object>> getTrends() { return service.getTopicTrends(); }
}