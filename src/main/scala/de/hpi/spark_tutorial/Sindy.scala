package de.hpi.spark_tutorial

import org.apache.spark.sql.catalyst.plans.physical.SinglePartition.numPartitions
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    //val inputs: List[String] = List("data/sindy_test.csv")
    //val inputs: List[String] = List("data/TPCH/tpch_customer.csv", "data/TPCH/tpch_nation.csv", "data/TPCH/tpch_region.csv",
    //  "data/TPCH/tpch_supplier.csv", "data/TPCH/tpch_part.csv")//, "data/sindy_test2.csv")

    //employees.show()
    //employees.explain()
    //print(employees.schema)
    //employees.select("A").show()

    val schema = StructType(
      StructField("Attribute", StringType, true) ::
        StructField("val", StringType, true) :: Nil
    )
    var all = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    var dfs: ArrayBuffer[DataFrame] = ArrayBuffer()
    var dfNames: ArrayBuffer[String] = ArrayBuffer()
    var dfInds: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    var dfIndsMap: collection.mutable.HashMap[String, ArrayBuffer[String]] = new collection.mutable.HashMap()

    var dfCounts: collection.mutable.HashMap[String,Int] = new collection.mutable.HashMap();


    var overall_count = 0
    var df_count = 0
    for(path <- inputs){
      val input = spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .csv(path)
      for(c <- input.columns){
        dfNames += c
        dfInds += ArrayBuffer()
        dfIndsMap(c) = ArrayBuffer()
        var df = input.select(c).withColumnRenamed(c, "val")
        df = df.withColumn("Attribute", lit(c))
        df = df.select("Attribute", "val")
        df = df.distinct()
        dfCounts(c) = df.count().toInt
        overall_count += dfCounts(c)
        //dfCounts = dfCounts + (c -> df.count().toInt)
        //dfs += df
        all = all.union(df)
        df_count+=1
      }
    }
    df_count-=1

    println("Overall count: " + overall_count.toString)

    //all = all.distinct()
    all = all.repartition(overall_count / 1000 + 1, all("val"))
    all = all.as("a1").join(all.as("a2")).where(col("a1.Attribute") < col("a2.Attribute") && col("a1.val") === col("a2.val"))
    all = all.groupBy("a1.Attribute", "a2.Attribute").count()


    val res = all.collect()
    for(item <- res){
      val a1 = item.getAs[String](0)
      val a2 = item.getAs[String](1)
      val count = item.getAs[Long]("count")
      //if(a1<a2){
        if(dfCounts(a1)==count){
          dfIndsMap(a1) += a2
        }
        if(dfCounts(a2)==count){
          dfIndsMap(a2) += a1
        }
      //}
    }


    /*for(a <- 0 to df_count){
      for(b <- 0 to a-1){
        if(a!=b){
          val dfA = dfs(a)
          val dfB = dfs(b)
          val join = dfA.join(dfB, usingColumn = "val")
          val len = join.count()
          println(dfNames(a) + "(" + dfA.count().toString + ") ," + dfNames(b) + "(" + dfB.count().toString + ") ->" + len.toString)
          if(len==dfA.count()){
            dfInds(a) += dfNames(b)
          }
          if(len==dfB.count()){
            dfInds(b) += dfNames(a)
          }
        }
      }
    }mk

    for(j <- 0 to df_count){
      if(dfInds(j).nonEmpty){
        print(dfNames(j) + " < " + dfInds(j).mkString(", "))
      }
    }*/

    dfIndsMap.foreach{
      case (key, value) => if(value.nonEmpty){println(key + " < " + value.mkString(", "))}
    }

    //all.show()

  }
}
