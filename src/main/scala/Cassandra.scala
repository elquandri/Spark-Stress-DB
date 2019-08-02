import com.datastax.spark.connector._
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


trait Cassandra {

  def cassandraStress(data: RDD[(String,String)],dfData: DataFrame, spark: SparkSession, conf: SparkConf): Unit ={

    cassandraWriter(data, conf)

    val dfRead = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "generate", "keyspace" -> "stress" ))
      .load()

    val diff = dfRead.except(dfData)

    if (diff.count()==0){
      println("sucess")
    }
    else {
      println("insucess")
      println(diff.count())
    }


  }

  def cassandraWriter(rdd: RDD[(String,String)], conf: SparkConf){

    CassandraConnector(conf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS stress ")
      session.execute("CREATE KEYSPACE stress WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE stress.generate (key text PRIMARY KEY, value text)")
    }
    rdd.saveToCassandra("stress", "generate", SomeColumns("key", "value"))

  }


}
