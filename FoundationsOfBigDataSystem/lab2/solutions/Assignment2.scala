package com.dataman.demo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


/**
  * @description: 大数据系统基础实验2，生成出现频率为10的倒排索引
  * @author     : harrypenny
  * @time       : 2016.6.5
  */


object Assignment1 {

   def main(args: Array[String]) = {

      val conf = new SparkConf().setAppName("assignment1")
    
      val sc = new SparkContext(conf)
    
      Logger.getRootLogger.setLevel(Level.WARN)

      val data = sc.textFile("hdfs://192.168.70.141:8020/Assignment1")
    
      //please code here.

      //val data = sc.textFile("test/doc.txt")

      val docs = data.map(line => (line.split(",")(0), line.split(",")(1)))


      //找出出现频率为10次的单词
      val wordCount = docs.flatMapValues(word => word.split(" ")).map{ 
                                   case (docId, word) => 
                                   (word, docId)}.distinct().
                                   groupByKey().mapValues{ docIds =>
                                      var cnt = 0
                                      val it = docIds.iterator
                                      while(it.hasNext){
                                         it.next()
                                         cnt += 1
                                      }
                                      cnt  
                                   }.filter{ wc => wc._2 == 10L}


      val words = docs.flatMapValues( word => word.split(" "))

      //单词位置
      val position = docs.mapValues( word => 
                               word.split(" ").length).flatMapValues{ count => 
                               (1 to count)}

      // 生成单词，位置的列表（word,List[(docId, position)])
      val wordWithPosition = words.zip(position).map{
                       case (word, position) =>
                       (word._1,(word._2, position._2))}.map{
                       case (docId, word) =>
                       (word._1, (docId, word._2))}.groupByKey()


      // 将List[(docId, position)]变为字符串 {docId1:[pos1, pos2,...]},{docId2:[pos1, pos2,...]},...
      val combinedPosition = wordWithPosition.mapValues{ pos =>
                       var docIds = collection.mutable.Buffer[String]()
                       var wordPos = collection.mutable.Buffer[String]()
                       val it = pos.iterator
                       while(it.hasNext){
                          val docp = it.next()
                          val str = docp._1
                          val p = docp._2.toString()
                          if (!docIds.contains(str)){
                              docIds += str 
                              wordPos += "{\"".concat(str ).concat("\":[").concat(p)
                          }else{
                              val idx = docIds.indexOf(str)
                              wordPos(idx) = wordPos(idx).concat(",").concat(p)
                          }   
                       }
                       val len = wordPos.length - 1
                       for (i <- 0 to len){
                          wordPos(i) = wordPos(i).concat("]}")
                       }
                       var posString = "["
                       val it3 = wordPos.iterator
                       while(it3.hasNext){
                          posString = posString.concat(it3.next()).concat(",")
                       }
                       posString = posString.substring(0, posString.length - 1)
                       posString = posString.concat("]")
                       posString
                       }



       // 将第一步找出的频率为10的单词与第三步生成的（word，postionString）做join，
       // 然后生成题目需要的json字符串生成最后的结果字符串
       val result = wordCount.join(combinedPosition).map{ case(word, pos) =>
                 var posString = "{\"".concat(word).concat("\":").
                                 concat(pos._2).concat("}")
                 posString
             }.collect().foreach(println)


       sc.stop()
  
   }
}
