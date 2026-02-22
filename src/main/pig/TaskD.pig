-- Load datasets
Pages = LOAD '$pagesPath' USING PigStorage(',') AS (PersonID, Name, Nationality, CountryCode, Hobby);
Friends = LOAD '$friendsPath' USING PigStorage(',') AS (FriendRel, PersonID, MyFriend, Date, FriendDesc);

-- Filter headers
Pages = FILTER Pages BY PersonID != 'PersonID';
Friends = FILTER Friends BY PersonID != 'PersonID';

-- COGROUP: Sends data from both files to the same reducer in one shuffle
Cogrouped = COGROUP Pages BY PersonID, Friends BY MyFriend;

-- Process the Bags in the Reducer
Result = FOREACH Cogrouped {
    -- Count how many friend records exist for this person
    friend_count = COUNT(Friends);
    GENERATE group AS PersonID, FLATTEN(Pages.Name) AS Name, friend_count AS TotalFriends;
}

STORE Result INTO '$outputPath' USING PigStorage('\t');