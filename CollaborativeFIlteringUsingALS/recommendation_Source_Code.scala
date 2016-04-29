import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel 
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val path2="/user/kpw150030/review_200mbfile.json"
val path="/user/kpw150030/input.json"
val review = sqlContext.read.json(path)
review.first()
review.show()
val ratings = review.map(x=>(x(1),x(7),x(4)))

var businesses=  review.map(x=>x(1)).filter(_!= null).distinct.collect()
val businessesList= businesses.map(x=>(x.toString))
businessesList.indexOf("W5mmO5HIk-qc0GzH6yhyyw") 
var users = review.map(x=>x(7)).filter(_!= null).distinct.collect()
val userList= users.map(x=>(x.toString))

def conversion: (Any) => Double = { case null=> 0.0  case l: Long => l case d: Double => d }
val ratingsNew=review.map(x=>org.apache.spark.mllib.recommendation.Rating(userList.indexOf(x(7)).toInt ,businessesList.indexOf(x(1)).toInt, conversion(x(4))))

val rank = 10
val numIterations = 10
val model = ALS.train(ratingsNew, rank, numIterations, 0.01) 
val usrBiz= ratingsNew.map { case org.apache.spark.mllib.recommendation.Rating(uId, bId, stars) =>  (uId, bId) }

val predictedValues =
  model.predict(usrBiz).map { case org.apache.spark.mllib.recommendation.Rating(user, bId, stars) =>((user, bId), stars) }
val starssAndPreds = ratingsNew.map { case org.apache.spark.mllib.recommendation.Rating(user, business, stars)=>  ((user, business), stars)}.join(predictedValues)
val MSE =starssAndPreds.map { case ((user, business), (r1, r2)) =>
  (r1 - r2)* (r1 - r2)
}.mean()

val RMSE = math.sqrt(MSE)

val uIdTobePredicted=starssAndPreds.first()

val usersRatings=ratingsNew.filter( { case org.apache.spark.mllib.recommendation.Rating(uId, bId, stars) => uId==248}).collect()
 
val topKRestaurents= model.recommendProducts(248, 8)

println(topKRestaurents.mkString("\n"))

