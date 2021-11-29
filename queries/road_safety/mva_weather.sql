SELECT
  Count(Accident_Severity) as total_accidents,
  Weather_Conditions_Desc,
  Casualty_Severity_Desc,
  Vehicle_Type_Desc
FROM
  christopherchalcraft_road_safety.g_mva
GROUP BY
  Weather_Conditions_Desc,
  Casualty_Severity_Desc,
  Vehicle_Type_Desc
ORDER BY total_accidents DESC