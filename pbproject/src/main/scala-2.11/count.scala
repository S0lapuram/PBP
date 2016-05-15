import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object count {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","E:\\MS\\PrinciplesofBigDataManagement\\hadoop-common-2.2.0-bin-master")
    // initialise spark context
    val conf = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory","8g")
    val sc = new SparkContext(conf)

    //val textFile = sc.textFile("E:\\MS\\PrinciplesofBigDataManagement\\Tweets.json")
    val sqlContext = new SQLContext(sc)
    val tweetsfile = sqlContext.jsonFile("E:\\MS\\PrinciplesofBigDataManagement\\python.json")
    tweetsfile.registerTempTable("querytable1")
    //val query1 = sqlContext.sql("select user.name, count(user.followers_count) as followersCount from querytable1 group by user.name order by followersCount desc limit 20")
    //query1.show()

    //piechart
    //val query1 = sqlContext.sql("select SUBSTRING(user.name,0,5), count(user.followers_count) as followersCount from querytable1 group by user.name order by followersCount desc limit 20")
    //query1.show()
    //query1.save("query1","json")

    //waterfall
    //val query2 = sqlContext.sql("select count(*) as cnt,retweeted_status.user.screen_name as name from querytable1 where retweeted_status.user.screen_name is not NULL group by retweeted_status.user.screen_name order by cnt desc limit 10")
    //query2.show()
    //query2.save("query2","json")

    //Donut chart
    val query3 = sqlContext.sql("select  user.lang as language, count(*) as cnt from querytable1 where user.lang is not NULL group by user.lang order by cnt desc limit 10")
    query3.show()
    query3.save("query3", "json")

    //Maps
    //val query4 = sqlContext.sql("select place.country_code as location,count(*) as cnt from querytable1 where place.country_code is not NULL and text like '%virat%' or text like '%kohli%' group by place.country_code order by cnt desc limit 10")
    //query4.show()
    //query4.save("query4","json")
/*
    val query5 = sqlContext.sql("SELECT source, count(*) as cnt FROM querytable1 WHERE source is not NULL and (user.verified OR NOT user.verified) GROUP BY source ORDER BY cnt DESC")
    query5.show()
    query5.save("query5","json")
*/
    //barchart
    //val query5 = sqlContext.sql("select user.name as name, retweeted_status.retweet_count as cnt from querytable1 where user.name is not NULL order by cnt desc limit 10")
    //query5.show()
    //query5.save("query5","json")
/*
    //val query6 = sqlContext.sql("select user.name as name, text, user.profile_image_url as image, coordinates,user.friends_count as friends from querytable1 where user.name is not NULL and coordinates is not NULL order by friends desc limit 10")
    //query6.show()
    //query6.save("query6","json")
*/

    //Line graph
    //val query6 = sqlContext.sql("select possibly_sensitive as sensitive,count(*) from querytable1 group by possibly_sensitive limit 3")
    //query6.show()
    //query6.save("query6","json")

    //Stepped graph
    //val query7 = sqlContext.sql("select substr(created_at,0,10) as time, count(*) as cnt from querytable1 where created_at is not NULL group by substr(created_at,0,10) order by cnt desc limit 10")
    //query7.show()
    //query7.save("query7","json")
/*
    val query8 = sqlContext.sql("select user.lang as language, count(*) as cnt from querytable1 where user.lang is not NULL and text like '%cricket%' and substr(created_at,0,10) in ('Thu Mar 31', 'Wed Mar 30') group by user.lang order by cnt desc limit 10")
    query8.show()
    query8.save("query8","json")
*/
    //Bubble graph
    //val query8 = sqlContext.sql("select user.time_zone as time, count(*) as cnt from querytable1 where user.time_zone is not NULL group by user.time_zone order by cnt desc limit 10")
    //query8.show()
    //query8.save("query8","json")

/*
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("E:\\MS\\PrinciplesofBigDataManagement\\hadoop-common-2.2.0-bin-master\\TweetsWordCount")
*/

  }
}
