package com.spnotes.spark.hbase

import com.mapr.org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunilpatil on 4/7/16.
  */
object HBaseOutputFormat {

  def main(argv: Array[String]): Unit = {
    if (argv.length != 3) {
      println("Please specify <inputfilename> <hbasetablename> <columnfamilyname>")
      System.exit(-1)
    }
    val inputFilePath = argv(0)
    val outputTableName = argv(1)
    val columnFamilyName = argv(2)

    val sparkConf = new SparkConf().setAppName("SparkHBaseWriter")
    val sparkContext = new SparkContext(sparkConf);

    val lines = sparkContext.textFile(inputFilePath)
    val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((first, second) => first + second)
    wordCount.foreach(println)

    println("Wordcount count " + wordCount.count())

    val configuration = HBaseConfiguration.create()
    configuration.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)

    val hBaseConfiguration = Job.getInstance(configuration)
    hBaseConfiguration.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    hBaseConfiguration.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)

    val hbaseRDD = wordCount.map { wordCount =>
      println(wordCount._1 + " -> " + wordCount._2)
      val word = Bytes.toBytes(wordCount._1)
      val put = new Put(Bytes.toBytes(wordCount._1 + ".123"))
      put.add(Bytes.toBytes("data"), Bytes.toBytes("word"), Bytes.toBytes(wordCount._1))
      put.add(Bytes.toBytes("data"), Bytes.toBytes("count"), Bytes.toBytes(wordCount._2))
      (word, put)
    }

    hbaseRDD.saveAsNewAPIHadoopDataset(hBaseConfiguration.getConfiguration)
    sparkContext.stop()
  }

}
