-- Load datasets
Pages = LOAD '$pagesPath' USING PigStorage(',') AS (PersonID, Name, Nationality, CountryCode, Hobby);
Friends = LOAD '$friendsPath' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, DateOfFriendship, FriendDesc);
AccessLog = LOAD '$accessLogPath' USING PigStorage(',') AS (AccessID, ByWho, WhatPage, Type, Time);

-- Filter headers
Pages = FILTER Pages BY PersonID != 'PersonID';
Friends = FILTER Friends BY PersonID != 'PersonID';
AccessLog = FILTER AccessLog BY AccessID != 'AccessID';

-- Get unique visits (Who visited which page)
Visits = FOREACH AccessLog GENERATE ByWho, WhatPage;
DistinctVisits = DISTINCT Visits;

-- Join friendship with visits
-- If a visit doesn't exist for the friendship, DistinctVisits::ByWho will be NULL
Joined = JOIN Friends BY (PersonID, MyFriend) LEFT OUTER, DistinctVisits BY (ByWho, WhatPage);
Missing = FILTER Joined BY DistinctVisits::ByWho IS NULL;

-- Join with Pages to get the name of the user who hasn't visited their friend
FinalJoin = JOIN Missing BY Friends::PersonID, Pages BY PersonID;
Result = FOREACH FinalJoin GENERATE Pages::PersonID, Name;
FinalResult = DISTINCT Result;

STORE FinalResult INTO '$outputPath' USING PigStorage('\t');