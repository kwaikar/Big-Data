//Question 3 : List	the	'user	id'	and	'stars'	of	users	that	reviewed	businesses	located in	Stanford.
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
 
val stanfordBusinesses=businessTuples.filter(x=>x(1).contains("Stanford")).map(x=>(x(0),1)) 
val reviewTupplesMapped=reviewTuples.map(x=> (x(2), (x(1),x(3)))) 
val stanfordRatings = stanfordBusinesses.join(reviewTupplesMapped).map({case (businessId, (count,(userId, rating))) =>(userId,rating)})

stanfordRatings.collect().foreach(println)
businessAndReview.saveAsTextFile("assignment3.output")
