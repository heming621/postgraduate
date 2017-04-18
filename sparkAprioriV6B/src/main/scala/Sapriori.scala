package com.alvinalexander.breakandcontinue
import util.control.Breaks._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark._
import scala.collection.immutable.Set
import java.text.SimpleDateFormat  
import java.util.Date
import java.io._
/*
REF: http://wanghuanming.com/2014/10/spark-apriori/
*/
object Sapriori{
    def main(args: Array[String]){
		var stime = System.currentTimeMillis()
        val DATA_PATH = "hdfs://zhm01:9000/user/root/data/mushroom.csv"
        val minSup = 0.20
        val conf = new SparkConf().setAppName("Sapriori")
                                  .set("spark.master","spark://zhm01:7077") //.set("spark.master","yarn")
								  .set("spark.driver.memory","28g")
                                  .set("spark.executor.memory","18g")
                                  .set("spark.cores.max","80")//.set("spark.executor.cores","8") //.set("spark.executor.instances","12")
                                  .set("spark.default.parallelism","80")
        val sc = new SparkContext(conf)
        var FIs = collection.mutable.ArrayBuffer[Array[(Set[Int], Int)]]()
        
        val rawTrans = sc.textFile(DATA_PATH,80).map(_.split(",").map(_.trim.toInt)).cache() // map: to each line; flatMap: to all lines 
                                                                                             // rawTrans: RDD[Array[Int]]
                                                                                             // .textFile->Array[String]; map->String,flatMap->Char
        val broadTrans = sc.broadcast(rawTrans.collect())                                    // broadTrans: Array[Array[Int]]
        val lenTrans = sc.broadcast(rawTrans.count())
        val minCount = sc.broadcast(rawTrans.count()*minSup)
        val oneFIS = rawTrans.flatMap(line=>line).map((_,1)).reduceByKey(_ + _).filter(_._2 > minCount.value) //RDD[(Int, Int)]
        println(s"\nlenOfTrans:${lenTrans.value}\n")
        println(s"\nrawTrans:$rawTrans\n")
        
        def getNowDate():String = {new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} 
        
        /////////////Fk-1 -> Ck////////////////
        def genCdds(freqItems:Array[(Set[Int], Int)], k:Int):Set[Set[Int]] = {
            //if(k<2)
            var fsets:Array[Set[Int]] = freqItems.map(x=>x._1) //if flatMap, return 'Array[Int]' not 'Array[(Set[Int]]'
            var cdds = Set[Set[Int]]()
            for{
                s1<-fsets
                s2<-fsets
                if s1!=s2
                if s1.toList.sorted.take(k-2)==s2.toList.sorted.take(k-2) //not (k-1)! 不然无候选集生成.//gen 3C, use 2F, compare 1 // Fk->Ck+1,Fk-1==Fk-1
            }cdds+=s1.union(s2)
            cdds
        }
        
        ////////////Ck->Fk////////////////////
        def supOrNot(trans:Set[Int], cdd:Set[Int]):Int = {if(cdd.subsetOf(trans)) 1 else 0}
        def validateCdds3(cdds:Set[Set[Int]]) = {
            rawTrans.flatMap(line => {        
            var tmp = Set[(Set[Int], Int)]()
            for(cdd <- cdds){
                tmp += cdd -> {if(cdd.subsetOf(line.toSet)) 1 else 0}
            }
            tmp                                
            }).reduceByKey(_ + _).filter(_._2 > minCount.value)
        }
        //////
        def validateCdds(cdds:Set[Set[Int]]) = {     // validateCdds(): RDD[(Set[Int], Int)]
            rawTrans.flatMap(line => {               // rawTrans -> sc.parallelize(broadTrans.value,12) 
                // flatMap:map+flatten-> Set[(Set[Int], Int)].flatten-> ( (Set[Int], Int), (Set[Int], Int), ...), not flatten first.//Get elements in "Set[]".
                var tmp = Set[(Set[Int], Int)]()
                for(cdd <- cdds){
                    tmp += cdd -> supOrNot(line.toSet, cdd)
                }
                tmp                                    //zi ->return tmp
            }).reduceByKey(_ + _).filter(_._2 > minCount.value)
        }
        
        ////////////////////////////////
        //1F: RDD[(Int,Int)]-> Array[(Set[Int], Int)], like Array((Set(36),6812),...)
        val freqItem = oneFIS.map(x => (Set(x._1),x._2)).collect()
        val keySet = freqItem.map(_._1)                     //Array(Set(36), Set(24),...)
        FIs += freqItem
        
        // generate 2C
        println(s"Generate 2 candidates. ${getNowDate}")
        var cdds = scala.collection.immutable.Set[Set[Int]]()
        for{i <- keySet;j <- keySet;if i != j}cdds += i.union(j)  //cdds += Set(i, j)
        println(s"Finished! And size of 2C is ${cdds.size}. ${getNowDate}")

        // 2C->2F
        println(s"Filter 2C -> 2F. ${getNowDate}")
        var freqItems = Array[(Set[Int], Int)]()
        freqItems = validateCdds3(cdds).collect()  //freqItems:RDD[(Set[Int], Int)].collect()-> Array[(Set[Int], Int)]
                                                  //results like "(Set(86, 76),0.5396356474643033)"
        println(s"Finished! And size of 2F is ${freqItems.size}. ${getNowDate}\n")
        // freqItems.sortBy(_._2).coalesce(1).saveAsTextFile("file:///home/zhm/sparkAprioriV2/data/freq2items")
        // freqItems.take(5).map(println) // freqItems.map(println)
        
        var k:Int = 3
        breakable{
            while(freqItems.size > 0){// && k<=6){ //??freqItems.length > 0?? freqItems.length != null ??
                FIs += freqItems
                //FIs.map(_.map(println))

                //->Ck //genCdds(freqItems:Array[(Set[Int], Int)], k:Int): Set[Set[Int]] //
				var tmpS1 = System.currentTimeMillis()
                println(s"Generate $k candidates. ${getNowDate}.")
                var lcdds:org.apache.spark.broadcast.Broadcast[scala.collection.immutable.Set[scala.collection.immutable.Set[Int]]] = null
                if(freqItems.size<=35000){
                    var rcdds = sc.parallelize(freqItems.map(x=>x._1),20).cartesian(sc.parallelize(freqItems.map(x=>x._1),20)).map(x=>(x._1++x._2)).filter(_.size==k)
                    lcdds = sc.broadcast(rcdds.collect().toSet)
                }else{
                    lcdds = sc.broadcast(genCdds(freqItems, k))
                }
                var rcdds = sc.parallelize(freqItems.map(x=>x._1),20).cartesian(sc.parallelize(freqItems.map(x=>x._1),20)).map(x=>(x._1++x._2)).filter(_.size==k)
				var lcdds = sc.broadcast(rcdds.collect().toSet)
				var tmpE1 = System.currentTimeMillis()
                println(s"Finished!              ${getNowDate}.") //And size of ${k}C is ${cdds.count()}. ${getNowDate}")
			    println(s"Timecost: ${(tmpE1-tmpS1)/1000}s. The size of ${k}C is ${lcdds.value.size}")
                
			    //Ck->Fk //def validateCdds(cdds:Set[Set[Int]]): RDD[(Set[Int], Int)]
				var tmpS2 = System.currentTimeMillis()
                println(s"Filter ${k}C -> ${k}F. ${getNowDate}")
                freqItems = validateCdds3(lcdds.value).collect()
				//// def supOrNot(trans:Set[Int], cdd:Set[Int]):Int = {if(cdd.subsetOf(trans)) 1 else 0}
				// freqItems = rcdds.repartition(100).map(cdd => {
				// 	var count:Int = 0
				// 	for(trans <- broadTrans.value){
			    //		if(cdd.subsetOf(trans.toSet)) count += 1;
				//	}
				//	cdd->count
				//}).reduceByKey(_ + _).filter(_._2 > minCount.value).collect()
				////
                lcdds.destroy()
				var tmpE2 = System.currentTimeMillis()
                println(s"Finished!        ${getNowDate}.")
				println(s"Timecost: ${(tmpE2-tmpS2)/1000}s. The size of ${k}F is ${freqItems.size}.\nThis iteration cost ${(tmpE1-tmpS1+tmpE2-tmpS2)/1000}s\n")
                k += 1
            }
        }
        //////    
        //FIs: ArrayBuffer[Array[(Set[Int], Int)]]
		var etime = System.currentTimeMillis()
        println(s"Timecost: ${(etime-stime)/1000}s.")
        val file = new File("/home/zhm/sparkAprioriV6B/FIs.txt")
        val bw = new BufferedWriter(new FileWriter(file))
        for(vl<-FIs.flatMap(line=>line).sortBy(-_._2)){
            bw.write(vl.toString+"\n")
        }
        println(s"Write into $file")
        bw.close()
        //
        sc.stop() 
    }
}

