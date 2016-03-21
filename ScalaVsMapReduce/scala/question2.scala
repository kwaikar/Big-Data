
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

val userNameFromConsole = scala.io.StdIn.readLine()

val usersSelected:String=userTuples.filter(x=>x(1).contains(userNameFromConsole)).take(1).map(x=>x(0)).take(1)(0)
val usersReviewTupples = reviewTuples.map(x=>(x(1),x(3).toDouble)).filter({case (id,rating)=>id.contains(usersSelected)})

val userIdAndMean = usersReviewTupples.groupByKey().map({case(id,rating)=>(id,mean(rating))})
userIdAndMean.collect()
 