
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.typesafe.config._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.BigDecimal
import org.apache.hadoop.conf.Configuration

object RedshiftLoadSample {
  def main(args: Array[String]) {
    val appConf = ConfigFactory.load()
    val conf = new SparkConf()
    .setAppName("RedshiftLoadSample")
    .setMaster( appConf.getConfig(args(2)).getString("deploymentMaster"))
    
    //Setup Redshift cluster properties
    val jdbcUsername = appConf.getConfig(args(2)).getString("redshiftUserName")
    val jdbcPassword = appConf.getConfig(args(2)).getString("redshiftUserPassword")
    val jdbcHostname = appConf.getConfig(args(2)).getString("redshiftClusterJdbcUrl")
    val jdbcPort = appConf.getConfig(args(2)).getString("redshiftDbPort")
    val jdbcDatabase = appConf.getConfig(args(2)).getString("redshiftDbName")
    val jdbcUrl = s"jdbc:redshift://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    //val jdbcUrl = "jdbc:redshift://mytestcluster.amy.us-west-1.redshift.amazonaws.com:5439/dev?user=masteer&password=5t"
    //println(jdbcUrl)
    val sc = new SparkContext(conf) 
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","")
    //sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    //sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", appConf.getConfig(args(2)).getString("aws_key"))
    //sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey",appConf.getConfig(args(2)).getString("aws_secret"))
    val sqlContext = new SQLContext(sc)
    val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", jdbcUrl)
    //.option("dbtable", "products")
    .option("query", "select count(*) from products")
    //.option("aws_iam_role", "arn:aws:iam::376622:role/Newredshiftrole")
    .option("forward_spark_s3_credentials","true")
    .option("tempdir", "s3n://albabucket/temp/")
    .load()
    df.show 
    /*
    formatted_updates.toDF().write
.format("com.databricks.spark.redshift")
.options("extracopyoptions", "TIMEFORMAT 'auto'")
.mode("append")
.save()
    val inP = args(0) //    val outP = args(1)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inPEx = fs.exists(new Path(inP)) //    val outPEx = fs.exists(new Path(outP))
    if (!inPEx){
      println ("No input directory")
      return
    }

    val ordersRDD = sc.textFile(inP+"/orders")
    val orderItemsRDD = sc.textFile(inP+"/order_items")
    val ordersCompleted = ordersRDD.filter(line => (line.split(",")(3) == "COMPLETE"))
    val orders = ordersCompleted.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
    val orderItemsMap = orderItemsRDD.map(ln => (ln.split(",")(1).toInt,ln.split(",")(4).toFloat))
    val orderItems = orderItemsMap.reduceByKey((accum, value) => accum + value)
    val orderJoin= orders.join(orderItems)
    //val h = orderJoin.take(5).foreach(rec => (println(rec._1 + "|||" + rec._2._1 + "|||" + rec._2._2)))
    //val orderJoinMap = orderJoin.map( rec = > (rec._2._1, rec._2._2))
    val ordersJoinMap = orderJoin.map(rec => (rec._2._1, rec._2._2))
        
        val revenuePerDay = ordersJoinMap.aggregateByKey((0.0, 0))(
       (accum, valu) => (accum._1+valu, accum._2 + 1),
       (tot1, tot2) => (tot1._1 + tot2._2, tot1._2 + tot2._2)
    )
    val rev = revenuePerDay.map(rec => (rec._1, BigDecimal(rec._2._1/rec._2._2).
        setScale(2,BigDecimal.RoundingMode.HALF_UP).toFloat ))
    val sorted = rev.sortByKey().take(2).foreach(println)
     
    */
  }
}
