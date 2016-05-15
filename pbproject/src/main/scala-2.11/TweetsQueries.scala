/**
  * Created by santu_pc on 4/7/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext


object TweetsQueries {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\MS\\PrinciplesofBigDataManagement\\hadoop-common-2.2.0-bin-master")
    // initialise spark context
    val sc = new SparkContext("local[2]","PbSpark")
    val sqlContext = new SQLContext(sc)
    val tweets = sqlContext.jsonFile("E:\\MS\\PrinciplesofBigDataManagement\\Vinutha\\tweets.csv")
    tweets.registerTempTable("tweets_1")
    //tweets.printSchema()
    //val tweet_s = sqlContext.sql("SELECT text FROM tweets_1 WHERE text <> '' ").map(r => r.getString(0))
    //val allcount = tweet_s.count()
    /*
    def time[A](f: => A) = {
      val s = System.nanoTime
      val ret = f
      println("query time: " + (System.nanoTime - s) / 1e9 + " sec")
      ret
    }
    time{
      sqlContext.sql("SELECT user.time_zone,count(*) as timeszone from tweets_1 where user.time_zone is not null group by user.time_zone").collect.foreach(println);
    }
    */
    val s1 = sqlContext.sql("SELECT user.time_zone,count(*) as timeszone from tweets_1 where user.time_zone is not null group by user.time_zone limit 10")
    s1.show()
    s1.save("Vquery1","json")
    //val res = s1.collect();
    //val abc = sc.parallelize(res);
    //abc.saveAsTextFile("C:\\Users\\raghu\\Desktop\\PBD_Phase2_Outputs\\Query4")
    //sc.stop()
  }

}
