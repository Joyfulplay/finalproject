package cs209a.finalproject.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AnalysisService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // 1. Topic Trends: 过去3年指定标签的月度提问量趋势
    public List<Map<String, Object>> getTopicTrends(String startDate, String endDate, List<String> tags) {
        // ========== 1. 参数合法性校验 ==========
        if (CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("分析标签列表不能为空，请至少指定一个标签");
        }
        try {
            LocalDate.parse(startDate);
            LocalDate.parse(endDate);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("时间格式错误，请使用 yyyy-MM-dd 格式（例如：2022-01-01）", e);
        }

        // ========== 2. 动态生成标签IN子句的占位符 ==========
        String tagPlaceholders = String.join(", ", tags.stream().map(tag -> "?").toArray(String[]::new));

        // ========== 3. 构建参数数组 ==========
        List<Object> params = new ArrayList<>();
        params.addAll(tags); // 第一步：标签参数（对应IN中的?）
        params.add(startDate); // 第二步：开始时间（对应第一个TO_DATE的?）
        params.add(endDate); // 第三步：结束时间（对应第二个TO_DATE的?）

        // ========== 4. 动态构建SQL ==========
        String sql = String.format("""
        SELECT
            to_char(to_timestamp(q.creation_date), 'YYYY-MM') AS month,
            qt.tag,
            count(*) AS question_count,
            sum(q.view_count) AS total_views,
            sum(q.score) AS total_score,
            sum(q.answer_count) AS total_answers,
            ROUND(SUM(CASE WHEN q.is_answered = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS answered_ratio
        FROM questions q
        JOIN question_tags qt ON q.question_id = qt.question_id
        WHERE qt.tag IN (%s)
          AND q.creation_date >= EXTRACT(EPOCH FROM TO_DATE(?, 'YYYY-MM-DD'))
          AND q.creation_date <= EXTRACT(EPOCH FROM (TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '1 DAY' - INTERVAL '1 SECOND'))
        GROUP BY month, qt.tag
        ORDER BY month ASC, qt.tag ASC
        """, tagPlaceholders);

        // ========== 5. 执行查询 ==========
        try {
            return jdbcTemplate.queryForList(sql, params.toArray());
        } catch (EmptyResultDataAccessException e) {
            return new ArrayList<>();
        }
    }

    // 2. Co-occurrence: Top N Java标签共现对（排除java标签）
    public List<Map<String, Object>> getCoOccurrence(int n) {
        String sql = """
            SELECT
                t1.tag as tag1,
                t2.tag as tag2,
                count(DISTINCT t1.question_id) as co_occurrence_count
            FROM question_tags t1
            JOIN question_tags t2 ON t1.question_id = t2.question_id
            WHERE
                t1.tag < t2.tag
                AND t1.tag != 'java'
                AND t2.tag != 'java'
            GROUP BY tag1, tag2
            ORDER BY co_occurrence_count DESC
            LIMIT ?
            """;
        return jdbcTemplate.queryForList(sql, n);
    }

    // 3. Multithreading Pitfalls: 分析多线程核心陷阱
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "the", "a", "an", "is", "are", "was", "were", "i", "you", "he", "she", "it",
            "we", "they", "in", "on", "at", "for", "with", "of", "to", "and", "or", "but",
            "how", "why", "what", "when", "where", "which", "who", "this", "that", "these", "those",
            "my", "your", "his", "her", "our", "their", "can", "could", "will", "would", "should",
            "java", "thread", "multithreading", "concurrency", "problem", "error", "issue", "bug"
    ));

    private static final Map<String, List<String>> PITFALL_RULES = new HashMap<>();
    static {
        PITFALL_RULES.put("Deadlock", Arrays.asList("deadlock", "deadlocks"));
        PITFALL_RULES.put("Race Condition", Arrays.asList("race condition", "race conditions"));
        PITFALL_RULES.put("ConcurrentModificationException", Arrays.asList("concurrentmodificationexception"));
        PITFALL_RULES.put("IllegalMonitorStateException", Arrays.asList("illegalmonitorstateexception"));
        PITFALL_RULES.put("Volatile Misuse", Arrays.asList("volatile"));
        PITFALL_RULES.put("Synchronization Issue", Arrays.asList("synchronized", "synchronization"));
        PITFALL_RULES.put("Thread Pool Error", Arrays.asList("thread pool", "threadpool", "executorservice", "threadpoolexecutor"));
        PITFALL_RULES.put("Thread Starvation", Arrays.asList("starvation", "starve"));
        PITFALL_RULES.put("Memory Visibility", Arrays.asList("memory visibility", "visibility issue"));
        PITFALL_RULES.put("InterruptedException", Arrays.asList("interruptedexception", "thread interrupt"));
    }

    // ========== 步骤1：自动挖掘多线程问题的高频候选关键词 ==========
    private List<Map.Entry<String, Long>> getHighFreqKeywords(int topN, int minOccurrence) {
        String sql = """
            SELECT
                lower(word) AS keyword,
                count(DISTINCT q.question_id) AS occurrence_count
            FROM (
                -- 拆分标题为单词
                SELECT q.question_id, regexp_split_to_table(q.title, '\\\\W+') AS word
                FROM questions q
                JOIN question_tags qt ON q.question_id = qt.question_id
                WHERE qt.tag IN ('multithreading', 'concurrency')
                UNION ALL
                -- 拆分内容为单词（排除HTML标签）
                SELECT q.question_id, regexp_split_to_table(regexp_replace(q.body, '<.*?>', ' ', 'g'), '\\\\W+') AS word
                FROM questions q
                JOIN question_tags qt ON q.question_id = qt.question_id
                WHERE qt.tag IN ('multithreading', 'concurrency')
            ) AS word_table
            WHERE
                word != ''
                AND length(word) >= 3
                AND lower(word) NOT IN (%s)
            GROUP BY keyword
            HAVING count(DISTINCT q.question_id) >= ?
            ORDER BY occurrence_count DESC
            LIMIT ?
            """;

        String stopWordPlaceholders = String.join(", ", STOP_WORDS.stream().map(w -> "?").toArray(String[]::new));
        String finalSql = String.format(sql, stopWordPlaceholders);

        List<Object> params = new ArrayList<>(STOP_WORDS);
        params.add(minOccurrence);
        params.add(topN);

        List<Map<String, Object>> rawResult = jdbcTemplate.queryForList(finalSql, params.toArray());

        return rawResult.stream()
                .map(item -> new AbstractMap.SimpleEntry<>(
                        (String) item.get("keyword"),
                        (Long) item.get("occurrence_count")
                ))
                .collect(Collectors.toList());
    }

    // ========== 步骤2：结合高频词+规则映射，生成多线程陷阱分析结果 ==========
    public List<Map<String, Object>> getMultithreadingPitfalls() {
        int topN = 50;
        int minOccurrence = 5;
        boolean includeManual = false;

        List<Map.Entry<String, Long>> highFreqKeywords = getHighFreqKeywords(topN, minOccurrence);

        Map<String, Long> pitfallCountMap = new HashMap<>();
        PITFALL_RULES.keySet().forEach(pitfall -> pitfallCountMap.put(pitfall, 0L));

        // 匹配高频词到陷阱类型
        for (Map.Entry<String, Long> keywordEntry : highFreqKeywords) {
            String keyword = keywordEntry.getKey();
            long count = keywordEntry.getValue();

            for (Map.Entry<String, List<String>> ruleEntry : PITFALL_RULES.entrySet()) {
                String pitfall = ruleEntry.getKey();
                List<String> ruleKeywords = ruleEntry.getValue();

                boolean isMatch = ruleKeywords.stream()
                        .anyMatch(ruleWord -> keyword.contains(ruleWord) || ruleWord.contains(keyword));

                if (isMatch) {
                    pitfallCountMap.put(pitfall, pitfallCountMap.get(pitfall) + count);
                    break;
                }
            }
        }

        // 补充手动配置的陷阱
        if (includeManual) {
            for (Map.Entry<String, List<String>> ruleEntry : PITFALL_RULES.entrySet()) {
                String pitfall = ruleEntry.getKey();
                List<String> ruleKeywords = ruleEntry.getValue();

                for (String ruleKeyword : ruleKeywords) {
                    String sql = """
                        SELECT count(DISTINCT q.question_id)
                        FROM questions q
                        JOIN question_tags qt ON q.question_id = qt.question_id
                        WHERE
                            qt.tag IN ('multithreading', 'concurrency')
                            AND (q.title ~* ? OR q.body ~* ?)
                        """;
                    Long manualCount;
                    try {
                        manualCount = jdbcTemplate.queryForObject(sql, Long.class, ruleKeyword, ruleKeyword);
                    } catch (EmptyResultDataAccessException e) {
                        manualCount = 0L;
                    }

                    if (manualCount > pitfallCountMap.get(pitfall)) {
                        pitfallCountMap.put(pitfall, manualCount);
                    }
                }
            }
        }

        // 转换返回格式
        return pitfallCountMap.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .map(entry -> {
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("pitfall_name", entry.getKey());
                    resultMap.put("occurrence_count", entry.getValue());
                    resultMap.put("relevance", entry.getValue() > minOccurrence ? "High" : "Low");
                    return resultMap;
                })
                .sorted((a, b) -> {
                    Long countA = (Long) a.get("occurrence_count");
                    Long countB = (Long) b.get("occurrence_count");
                    return countB.compareTo(countA);
                })
                .collect(Collectors.toList());
    }

    // 4. Solvability: 比较可解与难解问题的特征指标
    public List<Map<String, Object>> getSolvabilityComparison() {
        String sql = """
        WITH question_status_cte AS (
            SELECT
                q.*,
                u.reputation,
                CASE
                    WHEN q.accepted_answer_id IS NOT NULL
                         AND q.is_answered = TRUE
                         AND q.answer_count >= 1
                         THEN 'Solvable'
                    WHEN q.accepted_answer_id IS NULL
                         AND q.answer_count = 0
                         AND EXTRACT(DAY FROM NOW() - to_timestamp(q.creation_date)) > 30
                         THEN 'Hard-to-Solve'
                    ELSE 'Other'
                END as status
            FROM questions q
            JOIN users u ON q.owner_user_id = u.user_id
        )
        SELECT
            status,
            ROUND(AVG(reputation), 2) as avg_owner_reputation,
            ROUND(AVG(LENGTH(body)), 0) as avg_body_length,
            ROUND(SUM(CASE WHEN body LIKE '%<code>%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as code_snippet_ratio,
            ROUND(AVG(answer_count), 2) as avg_answer_count,
            ROUND(AVG(score), 2) as avg_score,
            ROUND(AVG((SELECT COUNT(*) FROM question_tags qt WHERE qt.question_id = question_status_cte.question_id)), 2) as avg_tag_count,
            ROUND(AVG(view_count), 2) as avg_view_count,
            ROUND(SUM(CASE WHEN EXISTS (
                SELECT 1 FROM question_tags qt
                WHERE qt.question_id = question_status_cte.question_id
                AND qt.tag IN ('multithreading', 'reflection', 'spring-boot')
            ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as complex_topic_ratio,
            COUNT(*) as total_questions
        FROM question_status_cte
        WHERE status IN ('Solvable', 'Hard-to-Solve')
        GROUP BY status
        """;
        return jdbcTemplate.queryForList(sql);
    }

    /**
     * 获取问题标签列表（去重），支持过滤条件
     */
    public List<Map<String, Object>> getTagList(
            Integer minOccurrence,
            Boolean excludeJava,
            Boolean orderByCount) {
        int finalMinOccurrence = (minOccurrence == null || minOccurrence < 1) ? 1 : minOccurrence;
        boolean finalExcludeJava = excludeJava == null || excludeJava;
        boolean finalOrderByCount = orderByCount == null || orderByCount;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("""
            SELECT
                qt.tag,
                count(DISTINCT qt.question_id) AS occurrence_count
            FROM question_tags qt
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();
        if (finalExcludeJava) {
            sqlBuilder.append(" AND qt.tag != ? ");
            params.add("java");
        }

        sqlBuilder.append("""
            GROUP BY qt.tag
            HAVING count(DISTINCT qt.question_id) >= ?
            """);
        params.add(finalMinOccurrence);

        if (finalOrderByCount) {
            sqlBuilder.append(" ORDER BY occurrence_count DESC ");
        } else {
            sqlBuilder.append(" ORDER BY qt.tag ASC ");
        }

        List<Map<String, Object>> rawResult = jdbcTemplate.queryForList(sqlBuilder.toString(), params.toArray());

        return rawResult.stream()
                .map(item -> {
                    Map<String, Object> tagMap = new HashMap<>();
                    tagMap.put("tag", item.get("tag"));
                    tagMap.put("occurrence_count", item.get("occurrence_count"));
                    long count = (Long) item.get("occurrence_count");
                    String hotLevel = count >= 1000 ? "Hot" : (count >= 100 ? "Medium" : "Low");
                    tagMap.put("hot_level", hotLevel);
                    return tagMap;
                })
                .collect(Collectors.toList());
    }

    /**
     * 无参数获取 Tag 列表（使用默认值）
     */
    public List<Map<String, Object>> getTagList() {
        return getTagList(null, null, null);
    }
}