-- Load datasets
Pages = LOAD '$pagesPath' USING PigStorage(',') AS (PersonID, Name, Nationality, CountryCode, Hobby);
Friends = LOAD '$friendsPath' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, DateOfFriendship, FriendDesc);

-- Filter headers
Pages = FILTER Pages BY PersonID != 'PersonID';
Friends = FILTER Friends BY PersonID != 'PersonID';

-- Count friends per person
Grouped = GROUP Friends BY MyFriend;
Counts = FOREACH Grouped GENERATE group AS PersonID, COUNT(Friends) AS FriendCount;

-- Calculate Global Average
AllCounts = GROUP Counts ALL;
AvgStat = FOREACH AllCounts GENERATE AVG(Counts.FriendCount) AS GlobalAvg;

-- Filter people above average (Using CROSS to compare to the single Avg row)
Filtered = FILTER (CROSS Counts, AvgStat) BY FriendCount > GlobalAvg;

-- Join with Pages to get names
Final = JOIN Filtered BY Counts::PersonID, Pages BY PersonID;
Result = FOREACH Final GENERATE Pages::PersonID, Name, FriendCount;

STORE Result INTO '$outputPath' USING PigStorage('\t');