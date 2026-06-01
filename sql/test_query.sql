WITH deduped AS (
    SELECT *
    FROM (
        SELECT
            tw.*,
            ROW_NUMBER() OVER (
                PARTITION BY window_start, window_end, keyword, source
                ORDER BY batch_id DESC, id DESC
            ) AS rn
        FROM trend_windows tw
    ) x
    WHERE rn = 1
), latest_window AS (
    SELECT MAX(window_end) AS max_window_end
    FROM deduped
), ranked AS (
    SELECT
        keyword,
        source,
        SUM(mention_count)::BIGINT AS mention_count,
        AVG(avg_sentiment)::DOUBLE PRECISION AS avg_sentiment,
        SUM(trend_score)::DOUBLE PRECISION AS trend_score,
        MIN(window_start) AS window_start,
        MAX(window_end) AS window_end
    FROM deduped
    WHERE window_end >= COALESCE((SELECT max_window_end FROM latest_window) - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')
    GROUP BY keyword, source
), source_ranked AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY trend_score DESC, mention_count DESC) as srn
    FROM ranked
)
SELECT source, count(*)
FROM source_ranked
WHERE srn <= 20
GROUP BY source;
