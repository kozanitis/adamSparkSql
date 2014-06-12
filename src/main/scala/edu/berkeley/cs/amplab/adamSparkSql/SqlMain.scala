/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adamSparkSql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object SqlMain{
  private var sqc: SQLContext = _

  def main(args: Array[String]){

    val conf = new SparkConf(true)
      .setMaster("local")
      .setAppName("AdamSparkSql")


    /*val conf = new SparkConf(true)  ///distributed mode
      .setMaster("spark://ec2-54-242-161-178.compute-1.amazonaws.com:7077")
      .setSparkHome("/root/spark")
      .setAppName("AdamSparkSql")*/

    sqc = new SQLContext(new SparkContext(conf))

    //sqc.parquetFile("hdfs://ec2-54-242-161-178.compute-1.amazonaws.com:9000/genomes/mouse/mouse.adam")
    sqc.parquetFile("file:///root/genomes/mouse.adam")
      .registerAsTable("MouseTable")

    sqc.sql("select * from mouseTable limit 56").collect.foreach(println)

    sqc.sparkContext.stop

  }


}
