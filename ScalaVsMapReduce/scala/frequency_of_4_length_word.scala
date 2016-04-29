-- Your	code	should	output	the	top	10	most	commonly	occurring	words	and	their	counts	for	four	 word	lengths.	It	is	up	to	you	how	you	implement	this	– either	have	the	word length	as	a	parameter	or	hard	code	it

-- Please provide path to the merged input file.


var  inputFile=sc.textFile("hdfs://cshadoop1/user/kpw150030/allVombined.txt")

val allWords = inputFile.flatMap(_.split("\n").flatMap(x=>x.split("\\W+"))).map(x=>(x,1)).reduceByKey(_+_)

val wordsOfSize4= allWords.filter(_.length==4).sortBy(_._2,false).take(10) 

businessAndReview.collect().distinct.foreach(println)