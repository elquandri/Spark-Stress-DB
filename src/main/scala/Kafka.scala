import java.util.concurrent.Executors
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.util.concurrent.TimeUnit.SECONDS



trait Kafka {



  def kafkaWriter(data : DataFrame, topic : String): Unit ={

    data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.16.10.45:1025,172.16.10.27:1025,172.16.10.27:1026")
      .option("topic", topic)
      .save()

  }

  def kafkaStress(spark : SparkSession, topic : String, dataGenerated : DataFrame, size : Long): Unit ={

   kafkaWriter(dataGenerated,topic)

    import spark.implicits._

    var data = spark.createDataset(spark.sparkContext.emptyRDD[(String,String)])
    var ds1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],dfKafkaSchema(List("key","value","topic","partition","offset","timestamp","timestampType")))

       ds1 = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "172.16.10.45:1025")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

       data = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]


      val query: StreamingQuery = data.writeStream
        .outputMode("append")
        .format("console")
        .start()


    Executors.newSingleThreadScheduledExecutor.
      scheduleWithFixedDelay(queryTerminator(query), size, 60 * 5, SECONDS)
    spark.streams.resetTerminated
    query.awaitTermination()

    assert(spark.streams.active.isEmpty)




    ds1.show()
    println(ds1.count())

    val diff = ds1.except(dataGenerated)

    if (diff.count()==0){
      println("Success")
    }
    else {
      println("Failure")
      println(diff.count())
    }

  }

  def dfKafkaSchema(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "key", dataType = BinaryType , nullable = true),
        StructField(name = "value", dataType = BinaryType, nullable = true),
        StructField(name = "topic", dataType = StringType, nullable = true),
        StructField(name = "partition", dataType = IntegerType, nullable = true),
        StructField(name = "offset", dataType = LongType, nullable = true),
        StructField(name = "timestamp", dataType = TimestampType, nullable = true),
        StructField(name = "timestampType", dataType = IntegerType, nullable = true)
      )
    )



  def queryTerminator(query: StreamingQuery) = new Runnable {
    def run = {
      println(s"Stopping streaming query: ${query.id}")
      query.stop
    }
  }



}
