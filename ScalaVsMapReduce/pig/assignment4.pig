-- List	the user_id, and name of	the	top	10	users	who	have	written	the	most reviews.

business =LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId:chararray, fullAddress:chararray, categories:chararray);
review =LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId:chararray, userId:chararray,businessId:chararray,stars:double);
user =LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId:chararray,name:chararray,url:chararray);

 
groupedReviewByUserId = GROUP review BY userId;
-- get userWise conts
reviewsWithUserCounts = FOREACH groupedReviewByUserId GENERATE $0,COUNT($1);
--join both relations
topTenUsers = JOIN user by userId, reviewsWithUserCounts by $0;

--sort by count
sortedTopTenUsers =  ORDER topTenUsers BY $4 DESC ;

--select top 10
topTenUserIds= LIMIT sortedTopTenUsers 10; 

topTenUserIdAndName = FOREACH topTenUserIds GENERATE $0,$1;
dump topTenUserIdAndName;
