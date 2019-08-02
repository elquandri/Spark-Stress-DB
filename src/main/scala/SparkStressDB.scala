import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}


import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkStressDB  extends App with Cassandra with MongoDB with Redis{


  val numRecords = args(0).toLong
  val db = args(1).toString
  val numberOfRecords = sizeStrToNumber(args(0))

  if (db != "Cassandra" && db != "MongoDB"){

    println("le nom du db est incorrect")
    throwsException("le nom du db est incorrect")

  }


  val conf = new SparkConf()
    .registerKryoClasses(Array(classOf[Text]))
    .setAppName("SparkGen")


  db match {

    case "Cassandra" =>  conf.set("spark.cassandra.connection.host","172.16.10.11,172.16.10.33,172.16.10.38")
    case "MongoDB" => conf.set("spark.mongodb.input.uri","mongodb://172.16.10.38:27017")
                          .set("spark.mongodb.input.database","stress")
                          .set("spark.mongodb.input.collection","generate")
                          .set("spark.mongodb.output.uri","mongodb://172.16.10.38:27017")
                          .set("spark.mongodb.output.database","stress")
                          .set("spark.mongodb.output.collection","generate")

  }


  val spark = SparkSession.builder
    .master("local")
    .config(conf)
    .getOrCreate()






  val sc = spark.sparkContext

  try {

    val parts = sc.defaultParallelism
    val recordsPerPartition = math.ceil(numberOfRecords.toDouble / parts.toDouble).toLong

    println("===========================================================================")
    println("===========================================================================")
    println(s"Input size: $numberOfRecords")
    println(s"Total number of records: $numRecords")
    println(s"Number of output partitions: $parts")
    println("Number of records/output partition: " + (numRecords / parts))
    println(s"records per partition: $recordsPerPartition")
    println("===========================================================================")
    println("===========================================================================")

    if (!(recordsPerPartition < Long.MaxValue)) {
      throwsException(" SparkStress Exception, records per partition > {Long.MaxValue}")
    }



  val dataset = sc.parallelize(1 to parts, parts).mapPartitionsWithIndex { case (index, _) =>
    val one = new Unsigned16(1)
    val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition.toLong)
    val recordsToGenerate = new Unsigned16(recordsPerPartition)

    val recordNumber = new Unsigned16(firstRecordNumber)
    val lastRecordNumber = new Unsigned16(firstRecordNumber)
    lastRecordNumber.add(recordsToGenerate)

    val rand = Random16.skipAhead(firstRecordNumber)

    val rowBytes: Array[Byte] = new Array[Byte](SDBInputFormat.RECORD_LEN)
    val key = new Array[Byte](SDBInputFormat.KEY_LEN)
    val value = new Array[Byte](SDBInputFormat.VALUE_LEN)

    Iterator.tabulate(recordsPerPartition.toInt) { offset =>
      Random16.nextRand(rand)
      generateRecord(rowBytes, rand, recordNumber)
      recordNumber.add(one)
      rowBytes.copyToArray(key, 0, SDBInputFormat.KEY_LEN)
      rowBytes.takeRight(SDBInputFormat.VALUE_LEN).copyToArray(value, 0,
        SDBInputFormat.VALUE_LEN)
      (key, value)
    }
  }

    val data = dataset.map(kv => convertBytesToHex2(kv._1,kv._2))
    val dataRow = data.map(line => Row.fromTuple(line))
    val dfData = spark.createDataFrame(dataRow,dfSchema(List("key","value")))
    dfData.show()


    db match {

      case "Cassandra" => cassandraStress(data,dfData,spark,conf)

      case "MongoDB"   => mongoDBStress(dfData,spark)

    }


  }
  catch{
    case e: Exception => println("SparkGen Exception" + e.getMessage() + e.printStackTrace())
  } finally {
    sc.stop()
  }



  def sizeStrToNumber(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1000
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }


  def sizeToSizeStr(size: Long): String = {
    val kbScale: Long = 1000
    val mbScale: Long = 1000 * kbScale
    val gbScale: Long = 1000 * mbScale
    val tbScale: Long = 1000 * gbScale
    if (size > tbScale) {
      size / tbScale + "TB"
    } else if (size > gbScale) {
      size / gbScale  + "GB"
    } else if (size > mbScale) {
      size / mbScale + "MB"
    } else if (size > kbScale) {
      size / kbScale + "KB"
    } else {
      size + "B"
    }
  }

  def generateRecord(recBuf: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      recBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    recBuf(10) = 0x00.toByte
    recBuf(11) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record.
    i = 0
    while (i < 32) {
      recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(44) = 0x88.toByte
    recBuf(45) = 0x99.toByte
    recBuf(46) = 0xAA.toByte
    recBuf(47) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      recBuf(48 + i * 4) = v
      recBuf(49 + i * 4) = v
      recBuf(50 + i * 4) = v
      recBuf(51 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(96) = 0xCC.toByte
    recBuf(97) = 0xDD.toByte
    recBuf(98) = 0xEE.toByte
    recBuf(99) = 0xFF.toByte
  }


  def throwsException(message: String) {
      throw new Exception(message);
    }




  def convertBytesToHex2(bytes1: Seq[Byte], bytes2: Seq[Byte]): (String, String) = {
    val sb1 = new StringBuilder
    val sb2 = new StringBuilder
    for (b <- bytes1) {
      sb1.append(String.format("%02x", Byte.box(b)))
    }
    for (b <- bytes2) {
      sb2.append(String.format("%02x", Byte.box(b)))
    }
    (sb1.toString,sb2.toString)
  }



  def dfSchema(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "key", dataType = StringType, nullable = false),
        StructField(name = "value", dataType = StringType, nullable = false)
      )
    )



}
