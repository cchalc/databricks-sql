SELECT
  COUNT(*) as sev_count,
  Accident_everity
FROM
  christopherchalcraft_road_safety.g_mva
GROUP BY
  Accident_Severity SORT BY Accident_Severity ASC
LIMIT
  1000S
