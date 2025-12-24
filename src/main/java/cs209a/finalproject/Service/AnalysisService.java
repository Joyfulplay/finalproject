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
        // 校验标签列表非空
        if (CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("分析标签列表不能为空，请至少指定一个标签");
        }
        // 校验时间格式（yyyy-MM-dd），避免无效时间戳转换
        try {
            LocalDate.parse(startDate);
            LocalDate.parse(endDate);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("时间格式错误，请使用 yyyy-MM-dd 格式（例如：2022-01-01）", e);
        }

        // ========== 2. 动态生成标签IN子句的占位符 ==========
        // 例如 tags = [spring-boot, lambda] → 生成 "?, ?"
        String tagPlaceholders = String.join(", ", tags.stream().map(tag -> "?").toArray(String[]::new));

        // ========== 3. 构建参数数组（时间参数 + 标签参数） ==========
        List<Object> params = new ArrayList<>();
        // 先添加时间参数（startDate/endDate 转换为秒级时间戳）
        params.add(startDate);
        params.add(endDate);
        // 再添加所有标签参数
        params.addAll(tags);

        // ========== 4. 动态构建SQL ==========
        String sql = String.format("""
        SELECT 
            to_char(to_timestamp(q.creation_date), 'YYYY-MM') AS month, 
            qt.tag, 
            count(*) AS question_count,                -- 问题发布量
            sum(q.view_count) AS total_views,          -- 总浏览量（关注度）
            sum(q.score) AS total_score,               -- 总得分（认可度）
            sum(q.answer_count) AS total_answers,      -- 总回答数（互动度）
            -- 已回答问题占比
            ROUND(SUM(CASE WHEN q.is_answered = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS answered_ratio
        FROM questions q 
        JOIN question_tags qt ON q.question_id = qt.question_id 
        -- 动态标签过滤（参数化）
        WHERE qt.tag IN (%s) 
          -- 时间范围：[startDate 00:00:00, endDate 23:59:59] 的秒级时间戳
          AND q.creation_date >= EXTRACT(EPOCH FROM TO_DATE(?, 'YYYY-MM-DD'))
          AND q.creation_date <= EXTRACT(EPOCH FROM (TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '1 DAY' - INTERVAL '1 SECOND'))
        GROUP BY month, qt.tag 
        ORDER BY month ASC, qt.tag ASC
        """, tagPlaceholders);

        // ========== 5. 执行查询 ==========
        try {
            return jdbcTemplate.queryForList(sql, params.toArray());
        } catch (EmptyResultDataAccessException e) {
            // 无数据时返回空列表，避免NPE
            return new ArrayList<>();
        }
    }

    // 2. Co-occurrence: Top N 标签共现对
    // 停用词列表（过滤无意义词汇）
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "the", "a", "an", "is", "are", "was", "were", "i", "you", "he", "she", "it",
            "we", "they", "in", "on", "at", "for", "with", "of", "to", "and", "or", "but",
            "how", "why", "what", "when", "where", "which", "who", "this", "that", "these", "those",
            "my", "your", "his", "her", "our", "their", "can", "could", "will", "would", "should",
            "java", "thread", "multithreading", "concurrency", "problem", "error", "issue", "bug"
    ));

    // 陷阱类型-关键词映射规则（可配置，结合业务认知）
    private static final Map<String, List<String>> PITFALL_RULES = new HashMap<>();
    static {
        // 核心陷阱类型 + 核心关键词（基础规则）
        PITFALL_RULES.put("Deadlock (死锁)", Arrays.asList("deadlock", "deadlocks"));
        PITFALL_RULES.put("Race Condition (竞态条件)", Arrays.asList("race condition", "race conditions"));
        PITFALL_RULES.put("ConcurrentModificationException (并发修改异常)", Arrays.asList("concurrentmodificationexception"));
        PITFALL_RULES.put("IllegalMonitorStateException (非法监视器状态)", Arrays.asList("illegalmonitorstateexception"));
        PITFALL_RULES.put("Volatile Misuse (Volatile使用错误)", Arrays.asList("volatile"));
        PITFALL_RULES.put("Synchronization Issue (同步问题)", Arrays.asList("synchronized", "synchronization"));
        PITFALL_RULES.put("Thread Pool Error (线程池问题)", Arrays.asList("thread pool", "threadpool", "executorservice", "threadpoolexecutor"));
        PITFALL_RULES.put("Thread Starvation (线程饥饿)", Arrays.asList("starvation", "starve"));
        PITFALL_RULES.put("Memory Visibility (内存可见性问题)", Arrays.asList("memory visibility", "visibility issue"));
        PITFALL_RULES.put("InterruptedException (中断异常)", Arrays.asList("interruptedexception", "thread interrupt"));
    }

    // ========== 步骤1：自动挖掘多线程问题的高频候选关键词 ==========
    private List<Map.Entry<String, Long>> getHighFreqKeywords(int topN, int minOccurrence) {
        // PostgreSQL 文本处理：拆分标题+内容为单词，过滤停用词，统计词频
        String sql = """
            SELECT 
                lower(word) AS keyword,
                count(DISTINCT q.question_id) AS occurrence_count
            FROM (
                -- 拆分标题为单词
                SELECT q.question_id, regexp_split_to_table(q.title, '\\W+') AS word
                FROM questions q
                JOIN question_tags qt ON q.question_id = qt.question_id
                WHERE qt.tag IN ('multithreading', 'concurrency')
                UNION ALL
                -- 拆分内容为单词（排除HTML标签，只保留文本）
                SELECT q.question_id, regexp_split_to_table(regexp_replace(q.body, '<.*?>', ' ', 'g'), '\\W+') AS word
                FROM questions q
                JOIN question_tags qt ON q.question_id = qt.question_id
                WHERE qt.tag IN ('multithreading', 'concurrency')
            ) AS word_table
            WHERE 
                -- 过滤空字符串、短单词（长度<3）、停用词
                word != '' 
                AND length(word) >= 3 
                AND lower(word) NOT IN (%s)
            GROUP BY keyword
            -- 过滤出现频次过低的关键词（避免噪音）
            HAVING count(DISTINCT q.question_id) >= ?
            -- 取Top N高频词
            ORDER BY occurrence_count DESC
            LIMIT ?
            """;

        // 动态生成停用词占位符（避免SQL注入）
        String stopWordPlaceholders = String.join(", ", STOP_WORDS.stream().map(w -> "?").toArray(String[]::new));
        String finalSql = String.format(sql, stopWordPlaceholders);

        // 构建参数：停用词列表 + 最小出现频次 + TopN
        List<Object> params = new ArrayList<>(STOP_WORDS);
        params.add(minOccurrence);
        params.add(topN);

        // 执行查询，获取高频词列表
        List<Map<String, Object>> rawResult = jdbcTemplate.queryForList(finalSql, params.toArray());

        // 转换为Entry列表（方便排序/匹配）
        return rawResult.stream()
                .map(item -> new AbstractMap.SimpleEntry<>(
                        (String) item.get("keyword"),
                        (Long) item.get("occurrence_count")
                ))
                .collect(Collectors.toList());
    }

    // ========== 步骤2：结合高频词+规则映射，生成多线程陷阱分析结果 ==========
    public List<Map<String, Object>> getMultithreadingPitfalls() {
        // 配置项（可通过配置文件/参数传入）
        int topN = 50;          // 挖掘Top50高频词
        int minOccurrence = 5;  // 忽略出现次数<5的关键词（过滤噪音）
        boolean includeManual = true; // 是否包含手动配置的低频但重要的陷阱

        // 步骤1：挖掘实际数据中的高频关键词
        List<Map.Entry<String, Long>> highFreqKeywords = getHighFreqKeywords(topN, minOccurrence);

        // 步骤2：构建“陷阱类型-出现次数”映射（先基于高频词匹配规则）
        Map<String, Long> pitfallCountMap = new HashMap<>();
        // 初始化所有陷阱类型的计数为0
        PITFALL_RULES.keySet().forEach(pitfall -> pitfallCountMap.put(pitfall, 0L));

        // 遍历高频词，匹配陷阱规则，累加计数
        for (Map.Entry<String, Long> keywordEntry : highFreqKeywords) {
            String keyword = keywordEntry.getKey();
            long count = keywordEntry.getValue();

            // 匹配对应的陷阱类型
            for (Map.Entry<String, List<String>> ruleEntry : PITFALL_RULES.entrySet()) {
                String pitfall = ruleEntry.getKey();
                List<String> ruleKeywords = ruleEntry.getValue();

                // 关键词匹配（包含/完全匹配）
                boolean isMatch = ruleKeywords.stream()
                        .anyMatch(ruleWord -> keyword.contains(ruleWord) || ruleWord.contains(keyword));

                if (isMatch) {
                    pitfallCountMap.put(pitfall, pitfallCountMap.get(pitfall) + count);
                    break; // 一个关键词只匹配一个陷阱类型
                }
            }
        }

        // 步骤3：补充手动配置的陷阱（即使数据中频次低，也保留）
        if (includeManual) {
            for (Map.Entry<String, List<String>> ruleEntry : PITFALL_RULES.entrySet()) {
                String pitfall = ruleEntry.getKey();
                List<String> ruleKeywords = ruleEntry.getValue();

                // 对每个手动关键词，单独查询实际出现次数（避免遗漏）
                for (String ruleKeyword : ruleKeywords) {
                    String sql = """
                        SELECT count(DISTINCT q.question_id) 
                        FROM questions q 
                        JOIN question_tags qt ON q.question_id = qt.question_id 
                        WHERE 
                            qt.tag IN ('multithreading', 'concurrency')
                            AND (q.title ~* ? OR q.body ~* ?)
                        """;
                    Long manualCount = jdbcTemplate.queryForObject(sql, Long.class, ruleKeyword, ruleKeyword);
                    manualCount = manualCount == null ? 0 : manualCount;

                    // 若手动关键词的计数 > 当前陷阱的计数，则更新（避免被高频词覆盖）
                    if (manualCount > pitfallCountMap.get(pitfall)) {
                        pitfallCountMap.put(pitfall, manualCount);
                    }
                }
            }
        }

        // 步骤4：转换为返回格式（关键修复：用HashMap显式指定<String, Object>）
        return pitfallCountMap.entrySet().stream()
                .filter(entry -> entry.getValue() > 0) // 过滤无数据的陷阱类型
                .map(entry -> {
                    // 显式创建HashMap<String, Object>，避免泛型推断冲突
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("pitfall_name", entry.getKey());
                    resultMap.put("occurrence_count", (Object) entry.getValue()); // 显式转Object
                    resultMap.put("relevance", entry.getValue() > minOccurrence ? "High" : "Low"); // 标注相关性
                    return resultMap;
                })
                .sorted((a, b) -> {
                    // 排序时显式转换类型，避免ClassCastException
                    Long countA = (Long) a.get("occurrence_count");
                    Long countB = (Long) b.get("occurrence_count");
                    return countB.compareTo(countA);
                })
                .collect(Collectors.toList());
    }

    // 4. Solvability: 比较可解与难解问题的特征指标
    public List<Map<String, Object>> getSolvabilityComparison() {
        String sql = """
        SELECT 
            -- 多维度定义问题分类
            CASE 
                WHEN q.accepted_answer_id IS NOT NULL 
                     AND q.is_answered = TRUE 
                     AND q.answer_count >= 1 
                     THEN 'Solvable'  -- 可解决：有采纳答案+已回答+至少1个回答
                WHEN q.accepted_answer_id IS NULL 
                     AND q.answer_count = 0 
                     AND EXTRACT(DAY FROM NOW() - to_timestamp(q.creation_date)) > 30 
                     THEN 'Hard-to-Solve'  -- 难解：无采纳+无回答+发布超30天
                ELSE 'Other'  -- 排除中间状态
            END as status,
            
            -- 特征1：用户特征 - 提问者平均声望
            ROUND(AVG(u.reputation), 2) as avg_owner_reputation,
            
            -- 特征2：内容特征 - 问题长度+代码片段占比（内容丰富度）
            ROUND(AVG(LENGTH(q.body)), 0) as avg_body_length,
            ROUND(SUM(CASE WHEN q.body LIKE '%<code>%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as code_snippet_ratio,
            
            -- 特征3：互动特征 - 平均回答数+平均得分
            ROUND(AVG(q.answer_count), 2) as avg_answer_count,
            ROUND(AVG(q.score), 2) as avg_score,
            
            -- 特征4：复杂度特征 - 平均标签数（标签越多越复杂）+ 平均浏览量
            ROUND(AVG((SELECT COUNT(*) FROM question_tags qt WHERE qt.question_id = q.question_id)), 2) as avg_tag_count,
            ROUND(AVG(q.view_count), 2) as avg_view_count,
            
            -- 特征5：复杂主题占比（多线程/反射等复杂标签）
            ROUND(SUM(CASE WHEN EXISTS (
                SELECT 1 FROM question_tags qt 
                WHERE qt.question_id = q.question_id 
                AND qt.tag IN ('multithreading', 'reflection', 'spring-boot')
            ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as complex_topic_ratio,
            
            -- 样本量
            COUNT(*) as total_questions
            
        FROM questions q 
        JOIN users u ON q.owner_user_id = u.user_id 
        -- 仅分析可解/难解两类
        WHERE status IN ('Solvable', 'Hard-to-Solve')
        GROUP BY status
        """;
        return jdbcTemplate.queryForList(sql);
    }
}