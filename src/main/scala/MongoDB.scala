import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

trait MongoDB {


  def mongoDBWriter (df:DataFrame) {

    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("Overwrite").save()

  }

  def mongoDBStress (data: DataFrame, spark: SparkSession): Unit ={

    mongoDBWriter(data)
    val df = MongoSpark.load(spark)
        df.show()
        println(df.count())
    val dfRead = df.drop("_id")
    val diff = dfRead.except(data)

    if (diff.count()==0){
      println("sucess")
    }
    else {
      println("insucess")
      println(diff.count())
    }


  }

}
