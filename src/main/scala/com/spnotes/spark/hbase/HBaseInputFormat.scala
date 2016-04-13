package com.spnotes.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkContext, SparkConf}
import com.mapr.org.apache.hadoop.hbase.util.Bytes

/**
  * Created by sunilpatil on 4/7/16.
  */
object HBaseInputFormat {

  def main(args: Array[String]) {
    if(args.length != 2){
      println("Please specify <hbasetablename> <columnfamilyname>")
      System.exit(-1)
    }

    val inputTableName = args(0)
    val columnFamilyName = args(1)

    println(s"Reading data from hbase $inputTableName $columnFamilyName")

    val sparkConf = new SparkConf().setAppName("HBaseInputFormat")
    val sparkContext = new SparkContext(sparkConf);

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName)
    val hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    println( "Number of records in table " + hbaseRDD.count())
    val wordCount = hbaseRDD.map{ record =>
      val word = record._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("word"))
      val count = record._2.getValue(Bytes.toBytes("data"), Bytes.toBytes("count"))
      (Bytes.toString(word), Bytes.toInt(count))
    }
    wordCount.foreach(record => println(s"$record._1 $record._2"))

  }
}
