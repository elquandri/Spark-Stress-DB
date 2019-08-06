import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait Redis {

  def redisWriter(df: DataFrame){

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "stress")
      .option("key.column", "key")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def redisStress(data: DataFrame, spark: SparkSession): Unit = {

    redisWriter(data)

    val dfRead = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "stress")
      .option("key.column", "key")
      .load()
    dfRead.show()

    val diff = dfRead.except(data)

    if (diff.count()==0){
      println("Success")
    }
    else {
      println("Failure")
      println(diff.count())
    }

  }

}
