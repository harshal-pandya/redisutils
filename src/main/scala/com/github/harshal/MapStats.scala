package com.github.harshal

import redis.clients.jedis.Jedis
import collection.JavaConverters._
import java.io.PrintWriter

/**
 * Methods to get statistics from a [String,Double] map
 * @author harshal
 * @date: 2/18/13
 */
class RedisUtils(host:String , port:Int ) extends Jedis(host:String , port:Int){

  /**
   * Return count of keys for a given value (Inverse map)
   */
  def inverseMap(value:Double):Seq[String] = {
    val keySet = keys("*").asScala
    keySet.flatMap(key => {
      get(key).toDouble match {
        case `value` => Seq(key)
        case _ => Nil
      }
    }).toSeq
  }

  def inverseMap(values:Set[Double]):Map[Double,Set[String]] = {
    val keySet = keys("*").asScala.toSet
    val temp = keySet.flatMap(key => {
      get(key).toDouble match {
        case v if(values(v)) => Seq((v,key))
        case _ => Nil
      }
    })
    temp.groupBy(_._1).map(kv=>kv._1->kv._2.map(_._2))
  }

  def inverseMap:Map[Double,Set[String]] = {
    readToMem.groupBy(_._2).map(kv=>kv._1->kv._2.map(_._1).toSet)
  }

  def readToMem:Map[String,Double]={
    val keySet = keys("*").asScala
    keySet.map(key=> key -> get(key).toDouble).toMap
  }

  def export(file:java.io.File,del:String="\t"){
    val writer = new PrintWriter(file)
    val keySet = keys("*").asScala
    try{
      for (key<-keySet){
        writer.write(key+del+get(key)+"\n")
      }
    }catch{
      case e:Exception => println(e.getStackTraceString)
    }finally {
      writer.close()
    }
  }

}

object RedisUtils{
  def main(args:Array[String])={
    assert(args.length==2,"Please pass host and port!!")
    val host = args(0).trim
    val port = args(1).toInt
    val utils = new RedisUtils(host,port)

    val res = utils.inverseMap(Set(1.0,2.0,3.0))
    println("Stats")
    println("Size : "+utils.dbSize())
    res.foreach(m=>println(m._1+" : "+m._2.size))
  }
}