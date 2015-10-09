import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds

val defaults = List("elephant" -> 1, "impala" -> 2, "kudu" -> 3)
val defrdd = sc.parallelize(defaults)

var ssc = new StreamingContext(sc,Seconds(5))
var mystream = ssc.socketTextStream("localhost",1234)

var words = mystream.flatMap(line => line.split("\\W"))
var wordCounts = words.map(x => (x, 1)).reduceByKey((x,y) => x+y)

var merged = wordCounts.transform { rdd => rdd.union(defrdd)}.reduceByKey((x,y) => x+y)

wordCounts.print()
merged.print()

ssc.start()
ssc.awaitTermination()
