import java.util.Properties
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer




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



    val props = createProp()
    val topics = List(topic)
    val consumer = new KafkaConsumer(props)


    try {
      consumer.subscribe(topics.asJava)
      var df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],dfKafkaSchema(List("key","value")))
      var count = 0

      while (count < size) {
        val records = consumer.poll(100)
        for (record <- records.asScala) {

          var dd = spark.sparkContext.parallelize(Seq((record.key().toString,record.value().toString)))
          var dataRow = dd.map(line => Row.fromTuple(line))
          var drecord = spark.createDataFrame(dataRow,dfKafkaSchema(List("key","value")))

          df = df.union(drecord)

          println("Topic: " + record.topic() +
            ",Key: " + record.key() +
            ",Value: " + record.value() +
            ", Offset: " + record.offset() +
            ", Partition: " + record.partition())

          count +=1
        }
      }

      df.show()

      val diff = df.except(dataGenerated)

      if (diff.count()==0){
        println("Success")
      }
      else {
        println("Failure")
        println(diff.count())
      }


    }catch{
      case e:Exception => e.printStackTrace()

    }finally {
      consumer.close()
    }



  }


  def createProp(): Properties  ={

    val props:Properties = new Properties()
    props.put("group.id", "test-consumer-group")
    props.put("bootstrap.servers","172.16.10.45:1025")
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")

    return props

  }

  def dfKafkaSchema(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "key", dataType = StringType , nullable = false),
        StructField(name = "value", dataType = StringType, nullable = false)

      )
    )





}
