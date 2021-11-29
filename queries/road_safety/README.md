# Databricks SQL Workshop Guide

Included in this document are the queries used for the SQLA workshop guide. This is intended to support the lectures and provided with no warranty or support.

All data and information in this document are public domain and openly available.

NOTE: Students should replace ‘######’ with their 6-digit student ID provided by Databricks
E.g. odl_student_499888@databricks.com would have the student ID ‘499888’

# Motor Vehicle Accident Summary
## Query 1
Name: ######_mva_summary
	SELECT COUNT(*) as sev_count, Accident_Severity
FROM `######_dbacademy`.g_mva
GROUP BY Accident_Severity
SORT BY Accident_Severity ASC
	Refresh: Never

### Visualization 1a
Name: Accidents by Severity - ######
	Type: Pie
	X Column: Accident_Severity
	Y Column: sev_count

# Motor Vehicle Accident Weather

## Query 2
Name: ######_mva_weather
	SELECT Count(Accident_Severity) as total_accidents, 
    Weather_Conditions_Desc, 
    Casualty_Severity_Desc, 
    Vehicle_Type_Desc
FROM `######_dbacademy`.g_mva
GROUP BY Weather_Conditions_Desc, 
    Casualty_Severity_Desc, 
    Vehicle_Type_Desc
	Refresh: Never

### Visualization 2a
Name: Casualty Severity by Weather - ######
	Type: Column
	X Column: Casualty_Severity_Desc
	Y Column: total_accidents
	Group By: Weather_Conditions_Desc
	Stacking: Stack

### Visualization 2b
Name: Accident Count by Weather - ######
	Type: Bar
	X Column: Weather_Conditions_Desc
	Y Column: total_accidents	

### Visualization 2c
Name: Vehicle Type by Weather - ######
	Type: Area
	X Column: Vehicle_Type_Desc
	Y Column: total_accidents
	Motor Vehicle Accident Vehicle Types

## Query 3
Name: ######_mva_vehicle_types
	SELECT Count(Vehicle_Type) as Total_Accidents,
    Vehicle_Type_Desc,
    Casualty_Severity_Desc,
    Vehicle_Manoeuvre_Desc
FROM `######_dbacademy`.g_mva
GROUP BY Vehicle_Type_Desc,
    Casualty_Severity_Desc,
    Vehicle_Manoeuvre_Desc
	Refresh: Never
	
### Visualization 3a
Name: Casualty Type by Vehicle Manoeuvre - ######
	Type: Bar
	X Column: Vehicle_Manoeuvre_Desc
	Y Column: Total_Accidents
	Group By: Casualty_Severity_Desc

# Motor Vehicle Accidents

## Query 4
Name: ######_mva_accidents
	SELECT Count(Vehicle_Type) as Total_Accidents,
    Vehicle_Type_Desc
FROM `######_dbacademy`.g_mva
GROUP BY Vehicle_Type_Desc
SORT BY Total_Accidents DESC
	Refresh: Never

*NOTE*: There is no visualization for this query. The output table will be placed directly in the dashboard later.

# Casualty Severity Categories

Now we will create a utility lookup. This will be used to add interactivity to the dashboard and visualizations. We will do this after we compose the initial dashboard.

## Query 5
Name: ######_Casualty_Sev_Categories
	SELECT Distinct(Casualty_Severity_Desc)
FROM `######_dbacademy`.g_mva 

	Refresh: Never

_______________
# Adding Interactivity
Interactivity can make dashboards and panels easier to navigate. We will modify the summary of accidents by vehicle type to choose an accident severity for breakdown rather than stacking the bar chart.

## Modify Query 3 
(######_mva_vehicle_types) and add a WHERE clause as follows:

SELECT Count(Vehicle_Type) as Total_Accidents,
    Vehicle_Type_Desc,
    Casualty_Severity_Desc,
    Vehicle_Manoeuvre_Desc
FROM `######_dbacademy`.g_mva
WHERE Casualty_Severity_Desc LIKE ''
GROUP BY Vehicle_Type_Desc,
    Casualty_Severity_Desc,
    Vehicle_Manoeuvre_Desc
	
Place the text cursor between the single quotes after the LIKE statement and click on the ellipses at the bottom-left corner of the window. This will allow us to insert a parameter selector.

Keyword: casualty_severity
	Title: Casualty severity (should be auto-filled)
	Type: Query Based Dropdown List
	Query: ######_Casualty_Sev_Categories
	
After you create the parameter value, select one of the casualty severity categories to review the results!

________________
# Create a Dashboard
In the Dashboard screen, create a new dashboard called ###### - Motor Vehicle Accidents History.

Add the following panels:

######_mva_summary
	Accidents by Severity - ######
	######_mva_weather
	Casualty Severity by Weather - ######
Accident Count by Weather - ######
Vehicle Type by Weather - ######
	######_mva_vehicle_types
	Casualty Type by Vehicle Manoeuvre - ######
	######_mva_accidents
	Table

Save the dashboard view
# Create an Alert
Alerts can be created on any table for any number of conditions. This is a really good way to check for values or criteria coming in.

Alerts should be predicated on a scheduled search.

## Query 6
Name: ######_pseudo_table
	-- Uncomment the following code block to create a pseudo table to alert on.
/**
DROP TABLE IF EXISTS ######_dbacademy.user;
CREATE TABLE ######_dbacademy.user (id INT, name STRING, age INT);
INSERT INTO ######_dbacademy.user VALUES (1, 'Amy', 35);
**/
	Refresh: Never
________________

## Query 7
Name: ######_alert
	SELECT COUNT(*) as count
FROM ######_dbacademy.user
	Refresh: 5 minutes
	
Create the new alert:
Query: ######_alert (choose from the dropdown)
	Trigger when: Count > {current top row value}
	Send Notification: Just once
	Template: Use default template

Run the following query to insert a row into the user table. Once the row count exceeds the value set in the alert definition, the alert will trigger.

## Query 8
Name: ######_alert_table_insert
	INSERT INTO ######_dbacademy.user VALUES (2, 'Andrew', 33);
	Refresh: Never

Next, either wait a few minutes for ######_alert to run again, or run it manually. If the return value exceeds the alert threshold, the alert will be triggered.

Triggered alerts can be viewed in the Alert page.