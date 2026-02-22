-- Load datasets
Pages = LOAD '$pagesPath' USING PigStorage(',') AS (PersonID, Name, Nationality, CountryCode, Hobby);
AccessLogs = LOAD '$accessLogPath' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, Type, Time);


-- Clean headers
Pages = FILTER Pages BY PersonID != 'PersonID';
AccessLogs = FILTER AccessLogs BY AccessID != 'AccessID';


-- COGROUP: Sends data from both files to the same reducer in ONE shuffle
Cogrouped = COGROUP Pages BY PersonID, AccessLogs BY WhatPage;


-- Aggregate and Join in the Reducer
JoinedCounts = FOREACH Cogrouped {
cnt = COUNT(AccessLogs);
GENERATE group AS PersonID, FLATTEN(Pages.Name) AS Name, cnt AS AccessCount;
}


-- Global Ranking (Requires a separate sorting stage)
Sorted = ORDER JoinedCounts BY AccessCount DESC;
Result = LIMIT Sorted 10;


STORE Result INTO '$outputPath' USING PigStorage('\t');

