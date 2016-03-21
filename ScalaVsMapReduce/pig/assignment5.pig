-- List	the		business_id, and	count	of	each	business's	ratings for the	businesses	that	 are	located	in	the	state	of	TX

business =LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId:chararray, fullAddress:chararray, categories:chararray);
review =LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId:chararray, userId:chararray,businessId:chararray,stars:double);
user =LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId:chararray,name:chararray,url:chararray);

 
groupedReviewByBusinessId = GROUP review BY businessId;
-- get businessId Wise conts
reviewsWithBusinessCounts = FOREACH groupedReviewByBusinessId GENERATE $0,COUNT($1);
--join both relations
businessesWithReviewCounts = JOIN business by businessId, reviewsWithBusinessCounts by $0;

texasBusinessess = FILTER businessesWithReviewCounts BY (fullAddress matches '.*TX.*'); 
texasBizWithRatingCount =  FOREACH texasBusinessess GENERATE $0,$4;

-- Sort by count - ASC and display on console.

distinctTexasBizWithRatingCount = DISTINCT texasBizWithRatingCount;
top =  ORDER distinctTexasBizWithRatingCount BY $1 ASC;
dump top;