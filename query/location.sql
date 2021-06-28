WITH _agg AS (
  SELECT
    location,
    STRUCT(
      SUM(Revenue) AS api,
      SUM(Revenue_del) AS manual,
      SUM(Revenue_diff) AS diff,
      SUM(Revenue_diff) / SUM(Revenue) AS perc_diff
    ) AS revenue
  FROM
    curb_tracking.deliverect_vs_dashboard_weekly
  GROUP BY
    location
)
SELECT
  location,
  ARRAY_AGG(STRUCT(name, value)) AS metrics
FROM
  _agg UNPIVOT(value FOR name IN (revenue))
GROUP BY
  location
ORDER BY location
