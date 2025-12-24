package cs209a.finalproject.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(AnalysisService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // 1. Topic Trends
    public List<Map<String, Object>> getTopicTrends(String startDate, String endDate, List<String> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("分析标签列表不能为空，请至少指定一个标签");
        }
        try {
            LocalDate.parse(startDate);
            LocalDate.parse(endDate);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("时间格式错误，请使用 yyyy-MM-dd 格式（例如：2022-01-01）", e);
        }

        String tagPlaceholders = String.join(", ", tags.stream().map(tag -> "?").toArray(String[]::new));
        List<Object> params = new ArrayList<>();
        params.addAll(tags);
        params.add(startDate);
        params.add(endDate);

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

        try {
            return jdbcTemplate.queryForList(sql, params.toArray());
        } catch (EmptyResultDataAccessException e) {
            log.warn("TopicTrends查询无结果，参数：tags={}, startDate={}, endDate={}", tags, startDate, endDate);
            return new ArrayList<>();
        }
    }

    // 2. Co-occurrence: Top N Java标签共现对（排除java标签）
    public List<Map<String, Object>> getCoOccurrence(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("Top N必须大于0");
        }
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
        try {
            return jdbcTemplate.queryForList(sql, n);
        } catch (EmptyResultDataAccessException e) {
            log.warn("CoOccurrence查询无结果，参数：n={}", n);
            return new ArrayList<>();
        }
    }

    // 3. Multithreading Pitfalls: 分析多线程核心陷阱
    // 扩展停用词列表：包含通用无意义词、Java语法关键字、高频无效编程词汇
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            // 基础停用词
            "the", "a", "an", "is", "are", "was", "were", "i", "you", "he", "she", "it",
            "we", "they", "in", "on", "at", "for", "with", "of", "to", "and", "or", "but",
            "how", "why", "what", "when", "where", "which", "who", "this", "that", "these", "those",
            "my", "your", "his", "her", "our", "their", "can", "could", "will", "would", "should",
            // 通用编程无效词
            "java", "thread", "multithreading", "concurrency", "problem", "error", "issue", "bug",
            // 高频无效词（从实际数据中提取）
            "new", "quot", "not", "have", "public", "class", "from", "void", "string", "there", "get",
            "code", "return", "out", "try", "system", "private", "any", "run", "while", "int", "one",
            "catch", "all", "println", "using", "use", "time", "after", "true", "want", "static",
            "start", "only", "add", "null", "way", "like", "call", "has", "method", "other", "some",
            "object", "does", "final", "need", "if", "else", "println", "break", "continue", "switch",
            "case", "default", "long", "short", "byte", "char", "boolean", "double", "float", "package"
    ));

    // 扩展多线程陷阱规则：覆盖所有核心陷阱类型，支持复合关键词匹配
    private static final Map<String, List<String>> PITFALL_RULES = new HashMap<>();
    static {
        PITFALL_RULES.put("Deadlock", Arrays.asList("deadlock", "deadlocks", "livelock"));
        PITFALL_RULES.put("Race Condition", Arrays.asList("race condition", "race conditions", "racecondition"));
        PITFALL_RULES.put("ConcurrentModificationException", Arrays.asList("concurrentmodificationexception"));
        PITFALL_RULES.put("IllegalMonitorStateException", Arrays.asList("illegalmonitorstateexception"));
        PITFALL_RULES.put("Volatile Misuse", Arrays.asList("volatile", "visibility", "memory visibility"));
        PITFALL_RULES.put("Synchronization Issue", Arrays.asList("synchronized", "synchronization", "lock", "locks"));
        PITFALL_RULES.put("Thread Pool Error", Arrays.asList("thread pool", "threadpool", "executorservice", "threadpoolexecutor"));
        PITFALL_RULES.put("Thread Starvation", Arrays.asList("starvation", "starve", "hungry"));
        PITFALL_RULES.put("InterruptedException", Arrays.asList("interruptedexception", "interrupt", "interrupted"));
        PITFALL_RULES.put("Virtual Thread Pinning", Arrays.asList("virtual thread", "virtual threads", "pinning"));
        PITFALL_RULES.put("Thread Blocking/Waiting", Arrays.asList("blocked", "waiting", "timed_waiting", "stuck", "hang"));
        PITFALL_RULES.put("Atomicity Issue", Arrays.asList("atomic", "atomicity", "non-atomic"));
        PITFALL_RULES.put("Spurious Wakeup", Arrays.asList("spurious", "wakeup", "spurious wakeup"));
    }

    // ========== 步骤1：自动挖掘多线程问题的高频候选关键词 ==========
    private List<Map.Entry<String, Long>> getHighFreqKeywords(int topN, int minOccurrence) {
        if (topN <= 0 || minOccurrence < 0) {
            throw new IllegalArgumentException("topN必须>0，minOccurrence必须>=0");
        }

        // 1. 拆分正则改为[^a-zA-Z-]+，保留连字符（如race-condition）
        // 2. 处理HTML转义字符（&#39;）
        // 3. 扩大标签范围（加入java），确保数据量
        // 4. 优化子查询别名引用，避免SQL语法错误
        String sql = """
            SELECT
                lower(word) AS keyword,
                count(DISTINCT word_table.question_id) AS occurrence_count
            FROM (
                -- Split title with HTML escape handling
                SELECT
                    q.question_id,
                    regexp_split_to_table(
                        regexp_replace(q.title, '&#39;', '''', 'g'),
                        '[^a-zA-Z-]+'  -- 仅拆分非字母/非连字符，保留复合关键词
                    ) AS word
                FROM questions q
                JOIN question_tags qt ON q.question_id = qt.question_id
                WHERE qt.tag IN ('multithreading', 'concurrency', 'java')
                UNION ALL
                -- Split body with HTML tag/escape handling
                SELECT 
                    q.question_id, 
                    regexp_split_to_table(
                        regexp_replace(regexp_replace(q.body, '<.*?>', ' ', 'g'), '&#39;', '''', 'g'),
                        '[^a-zA-Z-]+'
                    ) AS word
                FROM questions q
                JOIN question_tags qt ON q.question_id = qt.question_id
                WHERE qt.tag IN ('multithreading', 'concurrency')
            ) AS word_table
            WHERE
                word != ''
                AND length(word) >= 3
                AND lower(word) NOT IN (%s)
            GROUP BY keyword
            HAVING count(DISTINCT word_table.question_id) >= ?
            ORDER BY occurrence_count DESC
            LIMIT ?
            """;

        String stopWordPlaceholders = String.join(", ", STOP_WORDS.stream().map(w -> "?").toArray(String[]::new));
        String finalSql = String.format(sql, stopWordPlaceholders);
        log.debug("高频关键词查询SQL：{}", finalSql);

        List<Object> params = new ArrayList<>(STOP_WORDS);
        params.add(minOccurrence);
        params.add(topN);
        log.debug("高频关键词查询参数：{}", params);

        List<Map<String, Object>> rawResult;
        try {
            rawResult = jdbcTemplate.queryForList(finalSql, params.toArray());
        } catch (EmptyResultDataAccessException e) {
            log.warn("高频关键词查询无结果，参数：topN={}, minOccurrence={}", topN, minOccurrence);
            rawResult = new ArrayList<>();
        }

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
        boolean includeManual = true;

        List<Map.Entry<String, Long>> highFreqKeywords = getHighFreqKeywords(topN, minOccurrence);
        log.debug("挖掘到的高频关键词数量：{}", highFreqKeywords.size());

        Map<String, Long> pitfallCountMap = new HashMap<>();
        PITFALL_RULES.keySet().forEach(pitfall -> pitfallCountMap.put(pitfall, 0L));

        // 优化匹配逻辑：支持复合关键词（如"race condition"拆分为单个词匹配）
        for (Map.Entry<String, Long> keywordEntry : highFreqKeywords) {
            String keyword = keywordEntry.getKey().trim().toLowerCase();
            long count = keywordEntry.getValue();

            for (Map.Entry<String, List<String>> ruleEntry : PITFALL_RULES.entrySet()) {
                String pitfall = ruleEntry.getKey();
                List<String> ruleKeywords = ruleEntry.getValue();

                // 匹配逻辑：关键词包含规则词，或规则词（复合）包含关键词
                boolean isMatch = ruleKeywords.stream().anyMatch(ruleWord -> {
                    String ruleWordLower = ruleWord.toLowerCase();
                    if (ruleWordLower.contains(" ")) {
                        String[] ruleParts = ruleWordLower.split(" ");
                        return Arrays.stream(ruleParts).allMatch(keyword::contains);
                    } else {
                        return keyword.contains(ruleWordLower) || ruleWordLower.contains(keyword);
                    }
                });

                if (isMatch) {
                    pitfallCountMap.put(pitfall, pitfallCountMap.get(pitfall) + count);
                    log.debug("匹配到陷阱：{}，关键词：{}，计数+{}", pitfall, keyword, count);
                    break;
                }
            }
        }

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
                            qt.tag IN ('multithreading', 'concurrency', 'java')
                            AND (q.title ~* ? OR q.body ~* ?)
                        """;
                    Long manualCount;
                    try {
                        manualCount = jdbcTemplate.queryForObject(sql, Long.class, ruleKeyword, ruleKeyword);
                        manualCount = manualCount == null ? 0L : manualCount;
                    } catch (EmptyResultDataAccessException e) {
                        manualCount = 0L;
                    }

                    if (manualCount > pitfallCountMap.get(pitfall)) {
                        pitfallCountMap.put(pitfall, manualCount);
                        log.debug("手动补充陷阱计数：{}，计数={}", pitfall, manualCount);
                    }
                }
            }
        }

        // 转换结果并排序，过滤无数据的陷阱
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
    WITH question_code_length AS (
        SELECT
            q.question_id,
            q.body,
            q.creation_date,
            q.is_answered,
            q.answer_count,
            q.score,
            q.view_count,
            q.owner_user_id,
            q.accepted_answer_id,
            COALESCE(LENGTH(q.body), 1) AS body_total_length,
            COALESCE(SUM(LENGTH(code_content)), 0) AS code_total_length
        FROM questions q
        LEFT JOIN LATERAL (
            SELECT regexp_matches(q.body, '<code>(.*?)</code>', 'g') AS code_content_arr
        ) AS code_matches ON TRUE
        LEFT JOIN LATERAL (
            SELECT unnest(code_matches.code_content_arr) AS code_content
        ) AS code_content ON TRUE
        GROUP BY q.question_id, q.body, q.creation_date, q.is_answered, q.answer_count, 
                 q.score, q.view_count, q.owner_user_id, q.accepted_answer_id
    ),
    question_status_cte AS (
        SELECT
            qcl.*,
            u.reputation,
            CASE
                WHEN qcl.accepted_answer_id IS NOT NULL
                     AND qcl.is_answered = TRUE
                     AND qcl.answer_count >= 1
                     THEN 'Solvable'
                WHEN qcl.accepted_answer_id IS NULL
                     AND qcl.answer_count = 0
                     AND EXTRACT(DAY FROM NOW() - to_timestamp(qcl.creation_date)) > 30
                     THEN 'Hard-to-Solve'
                ELSE 'Other'
            END as status
        FROM question_code_length qcl
        JOIN users u ON qcl.owner_user_id = u.user_id
    )
    SELECT
        status,
        ROUND(AVG(reputation)::NUMERIC, 2) as avg_owner_reputation,
        ROUND(AVG(body_total_length)::NUMERIC, 0) as avg_body_length,
        ROUND(AVG(((code_total_length::FLOAT / body_total_length) * 100)::NUMERIC), 2) as code_snippet_ratio,
        ROUND(AVG(answer_count)::NUMERIC, 2) as avg_answer_count,
        ROUND(AVG(score)::NUMERIC, 2) as avg_score,
        ROUND(AVG((SELECT COUNT(*) FROM question_tags qt WHERE qt.question_id = question_status_cte.question_id))::NUMERIC, 2) as avg_tag_count,
        ROUND(AVG(view_count)::NUMERIC, 2) as avg_view_count,
        ROUND((SUM(CASE WHEN EXISTS (
            SELECT 1 FROM question_tags qt
            WHERE qt.question_id = question_status_cte.question_id
            AND qt.tag IN ('multithreading', 'reflection', 'spring-boot')
        ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::NUMERIC, 2) as complex_topic_ratio,
        COUNT(*) as total_questions
    FROM question_status_cte
    WHERE status IN ('Solvable', 'Hard-to-Solve')
    GROUP BY status
    ORDER BY status;
    """;
        try {
            return jdbcTemplate.queryForList(sql);
        } catch (EmptyResultDataAccessException e) {
            log.warn("SolvabilityComparison 查询无结果");
            return new ArrayList<>();
        }
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

        List<Map<String, Object>> rawResult;
        try {
            rawResult = jdbcTemplate.queryForList(sqlBuilder.toString(), params.toArray());
        } catch (EmptyResultDataAccessException e) {
            log.warn("TagList查询无结果，参数：minOccurrence={}, excludeJava={}", finalMinOccurrence, finalExcludeJava);
            rawResult = new ArrayList<>();
        }

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