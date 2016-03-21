def splitLine(line:String)={
val splits = line.split("\\^");
if(splits.size==3)
List(splits(0),splits(1),splits(2));
else
List(splits(0),splits(1),splits(2),splits(3));
}

def mean[T](s: Iterable[T])(implicit n: Fractional[T]) = n.div(s.sum, n.fromInt(s.size))

var businessData=sc.textFile("/yelpdatafall/business/business.csv")
var reviewData=sc.textFile("/yelpdatafall/review/review.csv")
var  userData=sc.textFile("/yelpdatafall/user/user.csv")

val businessTuples = businessData.flatMap(_.split("\n")).map(x=>splitLine(x))
val reviewTuples = reviewData.flatMap(_.split("\n")).map(x=>splitLine(x))
val userTuples = userData.flatMap(_.split("\n")).map(x=>splitLine(x))  

val businessIdAndAllRatings= reviewTuples.map(x=>(x(2),x(3).toDouble)).groupByKey()
val top10RatedBusinessesIds= sc.parallelize(businessIdAndAllRatings.map({case(id,rating)=>(id,mean(rating))}).sortBy(_._2,false).take(10)) 

val mappedBusinessTupples=businessTuples.map(x=> (x(0), (x(1),x(2)))) 

val addressDetailsOfTop10Businesses = mappedBusinessTupples.join(top10RatedBusinessesIds).map({case (businessId, ((address,categories),avergateRating)) =>(businessId,address,categories)})
addressDetailsOfTop10Businesses.collect().foreach(println)
 