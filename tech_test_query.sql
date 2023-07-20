-- select * from trades
--where login_hash = '0DF500A57244E5B1670277767BEBB554'
-- order by row_number desc;

-- select * from users

-- select DATE(open_time) from trades;
-- with p_s as (
-- select a.login_hash, a.server_hash, a.symbol, a.dt_report, 
-- sum(CASE WHEN dt_report >= dt_report - INTERVAL '7 days' THEN a.volume ELSE 0 END) over (partition by a.login_hash, a.server_hash, a.symbol) as sum_volume_prev_7d,
-- sum(a.volume)  over (partition by a.login_hash, a.server_hash, a.symbol) as sum_volume_prev_all,
-- sum(CASE WHEN dt_report >= '2020-08-01' and dt_report <= '2020-08-31' THEN a.volume ELSE 0 END) as sum_volume_2020_08
-- from
-- (select login_hash,server_hash,symbol,volume, DATE(open_time) as dt_report from trades) a
-- where   dt_report >= '2020-06-01' and dt_report <= 
-- '2020-09-30' --and login_hash = '0DF500A57244E5B1670277767BEBB554'
-- group by login_hash, server_hash, symbol, dt_report, volume
-- order by dt_report)
-- select login_hash, server_hash, symbol, dt_report, sum_volume_prev_7d, sum_volume_prev_all,
-- DENSE_RANK() OVER (partition by login_hash, symbol ORDER BY sum_volume_prev_7d DESC ) AS rank_volume_symbol_prev_7d,
-- DENSE_RANK() OVER (ORDER BY sum_volume_prev_7d, dt_report DESC) as rank_count_prev_7d
-- from p_s order by dt_report;

WITH all_dates AS (
  SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS dt_report
),
users_enabled AS (
  SELECT login_hash, currency
  FROM users
  WHERE enable = 1
),
all_combinations AS (
  SELECT
    dt_report,
    ue.login_hash,
    t.server_hash,
    t.symbol,
    ue.currency
  FROM all_dates
  CROSS JOIN users_enabled ue
  CROSS JOIN (
    SELECT DISTINCT server_hash, symbol
    FROM trades
  ) t
)
SELECT
  ac.dt_report,
  ac.login_hash,
  ac.server_hash,
  ac.symbol,
  ac.currency,
  COALESCE(SUM(CASE WHEN DATE(t.open_time) >= ac.dt_report - INTERVAL '7 days' AND DATE(t.open_time) <= ac.dt_report THEN t.volume ELSE 0 END), 0) AS sum_volume_prev_7d,
  COALESCE(SUM(CASE WHEN DATE(t.open_time) <= ac.dt_report THEN t.volume ELSE 0 END), 0) AS sum_volume_prev_all,
  DENSE_RANK() OVER (PARTITION BY ac.dt_report, ac.login_hash, ac.symbol ORDER BY COALESCE(SUM(t.volume), 0) DESC) AS rank_volume_symbol_prev_7d,
  DENSE_RANK() OVER (PARTITION BY ac.dt_report, ac.login_hash ORDER BY COUNT(t.volume) DESC) AS rank_count_prev_7d,
  COALESCE(SUM(CASE WHEN DATE(t.open_time) >= '2020-08-01' AND DATE(t.open_time) <= ac.dt_report THEN t.volume ELSE 0 END), 0) AS sum_volume_2020_08,
  MIN(ac.dt_report) OVER (PARTITION BY ac.login_hash, ac.server_hash, ac.symbol) AS date_first_trade,
  ROW_NUMBER() OVER (PARTITION BY ac.login_hash, ac.server_hash, ac.symbol ORDER BY ac.dt_report DESC) AS row_number
FROM all_combinations ac
LEFT JOIN trades t ON ac.dt_report = DATE(t.open_time) AND ac.login_hash = t.login_hash AND ac.server_hash = t.server_hash AND ac.symbol = t.symbol
GROUP BY ac.dt_report, ac.login_hash, ac.server_hash, ac.symbol, ac.currency
ORDER BY row_number DESC, ac.dt_report, ac.login_hash, ac.server_hash, ac.symbol;

	