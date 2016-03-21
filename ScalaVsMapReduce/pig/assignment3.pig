-- Q3.	List	the	'user	id'	and	'stars'	of	users	that	reviewed	businesses	located	in	Stanford.

business =LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId:chararray, fullAddress:chararray, categories:chararray);
review =LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId:chararray, userId:chararray,businessId:chararray,stars:double);
user =LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId:chararray,name:chararray,url:chararray);

allReviews = JOIN review by businessId, business by businessId;

stanfordBusinessess = FILTER allReviews BY (fullAddress matches '.*Stanford.*');
stanfordUserIdAndStars = FOREACH stanfordBusinessess GENERATE userId,stars;
dump stanfordUserIdAndStars;