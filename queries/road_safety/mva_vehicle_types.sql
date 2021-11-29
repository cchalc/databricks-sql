SELECT
  Count(Vehicle_Type) as Total_Accidents,
  Vehicle_Type_Desc,
  Casualty_Severity_Desc,
  Vehicle_Manoeuvre_Desc
FROM
  christopherchalcraft_road_safety.g_mva
-- WHERE Casualty_Severity_Desc LIKE '{{ casualty_severity }}'
GROUP BY
  Vehicle_Type_Desc,
  Casualty_Severity_Desc,