package cs209a.finalproject.Controller;

import cs209a.finalproject.Service.AnalysisService;
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
    public List<Map<String, Object>> getTrends(
            @RequestParam(defaultValue = "2022-01-01") String startDate,
            @RequestParam(defaultValue = "2025-12-31") String endDate,
            @RequestParam(defaultValue = "spring,hibernate,maven,lambda,stream") List<String> tags) {
        return service.getTopicTrends(startDate, endDate, tags);
    }

    // 辅助接口：给前端下拉框获取所有可用标签
    @GetMapping("/tags")
    public List<Map<String, Object>> getAvailableTags() {
        return service.getTagList(50, true, true);
    }
}
