package myself

import org.apache.spark.sql.SparkSession

object DBJoin extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

//  val employeesDF = spark.read
//    .format("jdbc")
//    .option("driver", "com.mysql.cj.jdbc.Driver")
//    .option("url", "jdbc:mysql://stage-vpc-audit-q3-2016-cluster.cluster-cazlhli6ii9k.us-east-1.rds.amazonaws.com/actionauditing")
//    .option("user", "audit-2018")
//    .option("password", "Tebnif7@Z8J38pNR7VYr")
//    .option("dbtable", "tbl_actions")
//    .load()
//
//  employeesDF.show()

//  val localDB = spark.read
//    .format("jdbc")
//    .option("driver", "com.mysql.cj.jdbc.Driver")
//    .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
//    .option("user", "root")
//    .option("password", "password")
//    .option("dbtable", "movies")
//    .load()
//
//  localDB.show()

  val moviesDF = spark.read.json("/Users/palaniappan/personal_projects/spark-essentials/src/main/resources/data/movies.json")

  moviesDF.write
    .format("jdbc")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
    .option("user", "root")
    .option("password", "password")
    .option("dbtable", "movies")
    .save()

}
