-- Q2.	Read	a	user	name	from	the	command	line	and	find	the	average	of	their	review	rating.


business =LOAD 'hdfs://cshadoop1/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId:chararray, fullAddress:chararray, categories:chararray);
review =LOAD 'hdfs://cshadoop1/yelpdatafall/review/review.csv' using PigStorage('^') as (reviewId:chararray, userId:chararray,businessId:chararray,stars:double);
user =LOAD 'hdfs://cshadoop1/yelpdatafall/user/user.csv' using PigStorage('^') as (userId:chararray,name:chararray,url:chararray);

userFromCommandLine = FILTER user BY name == 'Joanne G.'; 
selectedUsersReview = FILTER review by userId == userFromCommandLine.userId;

grouped = GROUP selectedUsersReview  ALL;
avgRatings = FOREACH grouped GENERATE AVG(selectedUsersReview.stars);

dump avgRatings;

