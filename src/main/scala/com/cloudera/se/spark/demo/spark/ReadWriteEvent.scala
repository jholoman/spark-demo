package com.cloudera.se.spark.demo.spark

import com.cloudera.se.spark.demo.model.SimpleEvent
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.avro.mapreduce.AvroJob
import org.apache.flume.source.avro.AvroFlumeEvent
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StringType, StructType}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by jholoman on 2/4/16.
  */
object ReadWriteEvent {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("ReadWriteEvent <readpath> <outputpath> <runLocal>")
      return
    }

    val readpath:String = args(0)
    val outputpath = args(1)
    val runLocal = (args.length == 3 && args(2).equals("T"))
    var sc:SparkContext = null
    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.cloudera.se.spark.demo.spark.AvroKryoRegistrator")
    val job = new Job()



    if (runLocal) {
      sc = new SparkContext("local", "ReadWriteEvent", sparkConf)
    } else {
      sparkConf.setAppName("ReadWriteEvent")
      sc = new SparkContext(sparkConf)
    }
    AvroJob.setInputKeySchema(job, AvroFlumeEvent.getClassSchema())
    var sqlContext = new SQLContext(sc)




    def rdd = sc.newAPIHadoopFile(readpath,
                 classOf[org.apache.avro.mapreduce.AvroKeyInputFormat[AvroFlumeEvent]],
                 classOf[org.apache.avro.mapred.AvroKey[AvroFlumeEvent]],
                 classOf[org.apache.hadoop.io.NullWritable],
                 job.getConfiguration())

    val rddRows:RDD[Row] = rdd.mapPartitions(events => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      events.map(record => {
        val arrayString = (deserialize(record._1.datum().getBody.array()))
        val se = (mapper.readValue(arrayString, classOf[SimpleEvent]))
        val seq = Seq(
        se.Headers.getHeader1,
        se.Headers.getHeader2,
        se.Headers.getHeader3,
        se.Headers.getHeader4,
        se.Body.getBody1())
        Row.fromSeq(seq)

      })
    })
    val schema =
      StructType(
        StructField("header1", StringType, false) ::
        StructField("header2", StringType, false) ::
        StructField("header3", StringType, false) ::
        StructField("header4", StringType, false) ::
        StructField("body", StringType, false) :: Nil)

    val df = sqlContext.createDataFrame(rddRows, schema)
    df.save(outputpath)
    }
  def deserialize(bytes: Array[Byte]) = new String(bytes)
}
