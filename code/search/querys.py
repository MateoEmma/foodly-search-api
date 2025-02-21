STATS_GENERAL_QUERY = """
    SELECT 
        COUNT(*) as total_searches,
        COUNT(DISTINCT user_id) as unique_users,
        AVG(results_count) as avg_results,
        AVG(execution_time_ms) as avg_execution_time,
        MIN(execution_time_ms) as min_execution_time,
        MAX(execution_time_ms) as max_execution_time,
        COUNT(CASE WHEN results_count = 0 THEN 1 END) as zero_results_searches
    FROM search_logs
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
"""

TOP_SEARCHES_QUERY = """
    SELECT 
        query,
        COUNT(*) as frequency,
        AVG(results_count) as avg_results
    FROM search_logs
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
    GROUP BY query
    ORDER BY frequency DESC
    LIMIT 10
"""

HOURLY_DISTRIBUTION_QUERY = """
    SELECT 
        HOUR(created_at) as hour,
        COUNT(*) as searches
    FROM search_logs
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
    GROUP BY HOUR(created_at)
    ORDER BY hour
"""