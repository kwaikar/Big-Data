-- List	the		business_id	,	full	address	and	categories	of	the	Top	10	highest	rated	 businesses	using	the	average	ratings.

business =LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId:chararray, fullAddress:chararray, categories:chararray);
review =LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId:chararray, userId:chararray,businessId:chararray,stars:double);
user =LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId:chararray,name:chararray,url:chararray);

 
groupedReviewByBusinessId = GROUP review BY businessId;
-- get businessId by average ratings
reviewsWithBusinessCounts = FOREACH groupedReviewByBusinessId GENERATE $0,AVG(review.stars);  


--join both relations
businessesWithReviewCounts = JOIN business by businessId, reviewsWithBusinessCounts by $0;
 
distinctBusinessesWithReviewCounts = DISTINCT businessesWithReviewCounts;

-- Sort by count - ASC and display on console.
 
sortedBusinessByAverage =  ORDER distinctBusinessesWithReviewCounts BY $4 DESC;
topTenBizByRating= LIMIT sortedBusinessByAverage 10; 
topTenBizByRatingFirstThreeCols = FOREACH topTenBizByRating GENERATE $0,$1,$2;
dump topTenBizByRatingFirstThreeCols;