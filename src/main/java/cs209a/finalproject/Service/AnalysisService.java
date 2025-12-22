package cs209a.finalproject.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class AnalysisService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // 1. Topic Trends: 过去3年指定标签的月度提问量趋势
    public List<Map<String, Object>> getTopicTrends() {
        // 1. 包含更多维度的 engagement 数据：问题数、总浏览量、总得分、总回答数
        // 2. 标签列表更新为你测试过的核心 Java 话题
        String sql = """
        SELECT 
            to_char(to_timestamp(q.creation_date), 'YYYY-MM') AS month, 
            qt.tag, 
            count(*) AS count,                     -- 问题发布量
            sum(q.view_count) AS total_views,      -- 总浏览量 (关注度)
            sum(q.score) AS total_score,           -- 总得分 (认可度)
            sum(q.answer_count) AS total_answers   -- 总回答数 (互动度)
        FROM questions q 
        JOIN question_tags qt ON q.question_id = qt.question_id 
        WHERE qt.tag IN ('java', 'spring-boot', 'lambda', 'generics', 'multithreading', 'stream-api', 'collections') 
          AND q.creation_date > 1640995200 -- 2022年之后的数据
        GROUP BY month, qt.tag 
        ORDER BY month ASC
        """;
        return jdbcTemplate.queryForList(sql);
    }

    // 2. Co-occurrence: Top N 标签共现对
    public List<Map<String, Object>> getCoOccurrence(int n) {
        String sql = """
            SELECT t1.tag as tag1, t2.tag as tag2, count(*) as weight 
            FROM question_tags t1 
            JOIN question_tags t2 ON t1.question_id = t2.question_id 
            WHERE t1.tag < t2.tag 
            GROUP BY tag1, tag2 
            ORDER BY weight DESC 
            LIMIT ?
            """;
        return jdbcTemplate.queryForList(sql, n);
    }

    // 3. Multithreading Pitfalls: 通过正则分析内容中的关键词频率
    public List<Map<String, Object>> getMultithreadingPitfalls() {
        String[] keywords = {"deadlock", "race condition", "memory leak", "synchronized", "volatile", "starvation", "thread safety"};
        List<Map<String, Object>> result = new ArrayList<>();
        for (String word : keywords) {
            // 使用 PostgreSQL 的 ~* (不区分大小写正则)
            String sql = """
                SELECT count(*) FROM questions q 
                JOIN question_tags qt ON q.question_id = qt.question_id 
                WHERE qt.tag = 'multithreading' 
                AND (q.body ~* ? OR q.title ~* ?)
                """;
            Long count = jdbcTemplate.queryForObject(sql, Long.class, word, word);
            result.add(Map.of("name", word, "value", count != null ? count : 0));
        }
        return result;
    }

    // 4. Solvability: 比较可解与难解问题的特征指标
    public List<Map<String, Object>> getSolvabilityComparison() {
        String sql = """
            SELECT 
                (CASE WHEN accepted_answer_id IS NOT NULL THEN 'Solvable' ELSE 'Hard-to-Solve' END) as status,
                AVG(u.reputation) as avg_reputation,
                AVG(LENGTH(q.body)) as avg_body_length,
                SUM(CASE WHEN q.body LIKE '%<code>%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as code_snippet_ratio
            FROM questions q 
            JOIN users u ON q.owner_user_id = u.user_id 
            GROUP BY status
            """;
        return jdbcTemplate.queryForList(sql);
    }
}