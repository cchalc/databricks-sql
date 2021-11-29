SELECT
  Count(Vehicle_Type) as Total_Accidents,
  Vehicle_Type_Desc
FROM
  christopherchalcraft_road_safety.g_mva
GROUP BY
  Vehicle_Type_Desc SORT BY Total_Accidents DESC