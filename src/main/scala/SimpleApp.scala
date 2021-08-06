/* SimpleApp.scala */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FsStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, Dataset,Column}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.{Row, SparkSession}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper

object SimpleApp {
  val topicIn = "AWSKafkaTutorialTopic"
  val bootstrapServers = "b-1.demo-cluster-1.0xk5hs.c2.kafka.ap-southeast-2.amazonaws.com:9092,b-2.demo-cluster-1.0xk5hs.c2.kafka.ap-southeast-2.amazonaws.com:9092"

  val version="0.1"
  val checkpointLocation = "s3://your-bucket/user/streaming/checkpoint/"
  val outFilePath = "s3://your-bucket/user/streaming/outPath/"
  val offsetOutPath = "s3://your-bucket/user/streaming/offsets"

  val queryName = "checkpointingTst"
  val appName = "AppName"

  var fs: FileSystem = null 
  var checkpointPath: Path = null

  val simpleSchema = StructType(Array(
    StructField("a",IntegerType,true),
  ))

  def main(args: Array[String]) {
    
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "MyRegistrator")

    val spark = SparkSession.builder.appName(appName).getOrCreate()
    val sparkContext = spark.sparkContext
    
    sparkContext.setLogLevel("WARN")

    fs = new Path(checkpointLocation).getFileSystem(sparkContext.hadoopConfiguration)
    checkpointPath = manageCheckpointLocation(fs, new Path(checkpointLocation),version)

    startStreaming(spark)

  }

  // this is very inefficient. best to place a sort of partitioning so we don;t have to scan the path, Somthing with compaction :)
  // if there is not offset checkpoint, we start from earliest
  //
  def getLastWrittenOffset(spark: SparkSession): String = {
    val fs = new Path(offsetOutPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val oPath = new Path(offsetOutPath)
    if ( fs.exists(oPath) && fs.isDirectory(oPath)) {
      val fragmentedMsgs = spark.read.format("avro").load(oPath.toString)
      val offsets: Array[Row] = fragmentedMsgs
        .select("partition", "offset","topic")
        .groupBy("partition","topic")
        .agg(
          max("offset").alias("offset")
        ).collect()
      var topics = scala.collection.mutable.Map[String,Map[String,Long]]()
      for ((v) <- offsets) {
        val ttp = v.getAs[String]("topic")
        val tof = v.getAs[Long]("offset")
        val tpart = v.getAs[Int]("partition").toString
        try {
          topics(ttp) = topics(ttp) + (tpart -> tof)
        } catch {
          case e: NoSuchElementException => topics += ( ttp -> Map(tpart->tof))
        }
      }
    
      if (topics.size == 0) {
        return "earliest"
      }
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      return mapper.writeValueAsString(topics)
    }
    return "earliest"
  }
  def startStreaming(spark: SparkSession): Unit = {

     val  startOffset = getLastWrittenOffset(spark);
     println("Starting from: " + startOffset)

     val kafkaMessages: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicIn)
      .option("enable.auto.commit",false)
      .option("startingoffsets", getLastWrittenOffset(spark))
      .load()

      import spark.implicits._
      val message = kafkaMessages.selectExpr("partition", "offset", "CAST(value AS STRING)", "timestamp").as[KafkaMessage]
      // TO BE DONE
      // If Anything in the foreachBatch fails, the foreach batch will be retried with the same batchId. 
      // It is important to records and safeguard from writing output records with the same batch Id. 
      // At this point we are just writing results with the batch id. there are no checks for duplicate writes/
      // For exctly-once, we should check if the batchId has ran before ( by checking a state store or results ), if yes, we should eigther skip this batch or write to a dead letter path to be processed later
      //
      def myFunction( batchDF:Dataset[KafkaMessage], batchID:Long ) : Unit = {
       batchDF.persist()
       val entities: Dataset[StreamEntity] = batchDF
                    .select(from_json(col("value"), simpleSchema).as("json_str"))
                    .select("json_str.*")
                    .as[StreamEntity]
       val entitiesWithBatchId = entities.withColumn("batchid",lit(batchID))
       entitiesWithBatchId.show()

       entitiesWithBatchId.write.mode("append").format("parquet").save(outFilePath)

       val lastOffset: DataFrame = batchDF
                  .select("partition", "offset")
                  .groupBy("partition")
                  .agg( max("offset").alias("offset"))
                  .withColumn("batchid",lit(batchID))
                  .withColumn("topic",lit(topicIn))

       lastOffset.show()
       lastOffset.write.partitionBy("topic").mode("append").format("avro").save(offsetOutPath)

       batchDF.unpersist()
      }

      val fileStream: StreamingQuery = message.writeStream
        .option("checkpointLocation", checkpointPath.toString)
        .queryName(queryName)
        .foreachBatch(myFunction _)
        .start()
      fileStream.awaitTermination()
  }

  def manageCheckpointLocation(fs: FileSystem ,parentPath: Path , version: String ) : Path = {
      val newLocation: String = parentPath.toString+"/"+version

      val path : Path = new Path(newLocation)
      if (fs.exists(path) && fs.isDirectory(path)) {
        return path
      } else {
        fs.mkdirs(path)
        return path
      }
  }
}
