def splitLine(line:String)={
val splits = line.split("\\^");
if(splits.size==3)
List(splits(0),splits(1),splits(2));
else
List(splits(0),splits(1),splits(2),splits(3));
}

var businessData=sc.textFile("/yelpdatafall/business/business.csv")
var reviewData=sc.textFile("/yelpdatafall/review/review.csv")
var  userData=sc.textFile("/yelpdatafall/user/user.csv")
val businessTuples = businessData.flatMap(_.split("\n")).map(x=>splitLine(x))
val reviewTuples = reviewData.flatMap(_.split("\n")).map(x=>splitLine(x))
val userTuples = userData.flatMap(_.split("\n")).map(x=>splitLine(x))

val userIdWiseReviewCountsSortedByCount=reviewTuples.map(x=> (x(1), 1)).reduceByKey(_+_).sortBy(_._2,false).take(10) 
val userIdWiseReviewCountsSortedByCountRDD = sc.parallelize(userIdWiseReviewCountsSortedByCount)
val userIdAndName= userTuples.map(x=>(x(0),x(1)))

val listOfUserIdNameAndReviewCount = userIdAndName.join(userIdWiseReviewCountsSortedByCountRDD).map({case (userId, (name, rating)) => (userId,name) })
val q4_Output=listOfUserIdNameAndReviewCount 

listOfUserIdNameAndReviewCount.collect().foreach(println) 