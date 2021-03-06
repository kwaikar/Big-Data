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
 
val texasBusinesses=businessTuples.filter(x=>x(1).contains("TX")).map(x=>(x(0),1)) 
val businessReviewCounts=reviewTuples.map(x=> (x(2), 1)).reduceByKey(_+_) 

val businessAndReview = businessReviewCounts.join(texasBusinesses).map({case (businessId, (reviewCount, businessCount)) =>(businessId,reviewCount)})
businessAndReview.saveAsTextFile("assignment5.output")