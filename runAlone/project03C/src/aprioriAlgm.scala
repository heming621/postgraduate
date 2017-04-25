import scala.collection.immutable.Set
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Set
import java.text.SimpleDateFormat  
import java.util.Date
import java.io._
/*
REF: http://wanghuanming.com/2014/10/spark-apriori/
*/
object aprioriAlgm{
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
		var vTmp = Set[(Set[Int], Int)]()
        def validateCdds(rawTrans:ArrayBuffer[Set[Int]], cdds:Set[Set[Int]], minCount:Int):Array[(Set[Int],Int)] = {
            rawTrans.flatMap(line => {
                //var tmp = Set[(Set[Int], Int)]()
				vTmp = Set()
                for(cdd <- cdds){
                    vTmp += cdd -> {if(cdd.subsetOf(line)) 1 else 0}//supOrNot(line, cdd)
                }
				//System.gc()
                vTmp
            }).groupBy(_._1).map(x=>(x._1, x._2.map(_._2).reduce(_+_))).filter(_._2 > minCount).toArray  // Map->Array
        }

        def validateCdds3(rawTrans:ArrayBuffer[Set[Int]], cdds:Set[Set[Int]], minCount:Int):Array[(Set[Int],Int)] = {
            rawTrans.flatMap(line => {
                var tmp = Set[(Set[Int], Int)]()
                for(cdd <- cdds){
                    tmp += cdd -> supOrNot(line, cdd)
                }
                tmp
            }).groupBy(_._1).map(x=>(x._1, x._2.map(_._2).reduce(_+_))).filter(_._2 > minCount).toArray  // Map->Array
        }


    def getNowDate():String = {new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())}
    
    def main(args: Array[String]){
        var times = System.currentTimeMillis()
        val DATA_PATH = "/home/mountDir/downloads/data/T40I10D100K.dat"
        val minSup = 0.10
        var FIs = ArrayBuffer[Array[(Set[Int], Int)]]()
        var cdds = Set[Set[Int]]()
        var freqItems = Array[(Set[Int], Int)]()
        ////------ READ DATA 
        val bufferedSource = Source.fromFile(DATA_PATH)
        var rawTrans:ArrayBuffer[Set[Int]] = ArrayBuffer[Set[Int]]()  // null? // 如Array/List不可变, import ...ArrayBuffer。
        var count:Int = 0
        for(line<-bufferedSource.getLines){
            count += 1
            rawTrans += line.split(" ").map(_.toInt).toSet
        }
        val minCount = (minSup*count).toInt
        println(minCount)
        ////------ Generate 1F
        val oneFIS = rawTrans.flatMap(line=>line).map((_,1)).groupBy(_._1).map(x=>(x._1, x._2.map(_._2).reduce(_+_))).filter(_._2 > minCount)
        freqItems = oneFIS.toArray.map(x => (Set(x._1),x._2))  // oneFIS:Map[Int,Int]
        FIs += freqItems
        ////------ Generate 2C, 2F
        val keySet = freqItems.map(_._1)  //Array[Set[Int]]
        println(s"Gen 2C.               ${getNowDate}")
        for{i <- keySet; j <- keySet; if i != j} cdds += i.union(j)
        println(s"GG! Size of 2C is ${cdds.size}. ${getNowDate}")
		//System.gc()
        freqItems = validateCdds(rawTrans, cdds, minCount)     //Array[(Set[Int], Int)] = Array((Set(34, 86),7906), ...)
        println(s"2C -> 2F\nGG! Size of 2F is ${freqItems.size}. ${getNowDate}\n")
        ////------ 
        var k:Int = 3
        while(freqItems.size > 0){ // && k <= 4){ //??freqItems.length > 0?? freqItems.length != null ??
                FIs += freqItems
                //->Ck //genCdds(freqItems:Array[(Set[Int], Int)], k:Int): Set[Set[Int]]
                var tmpS1 = System.currentTimeMillis(); println(s"Gen ${k}-cdds.")
                cdds = genCdds(freqItems, k)   
                var tmpE1 = System.currentTimeMillis(); println(s"GG！ Size of ${k}C is ${cdds.size}. Timecost:${(tmpE1-tmpS1)/1000}s")           
                //Ck->Fk //validateCdds(X, cdds:Set[Set[Int]], X):Array[(Set[Int],Int)]
                var tmpS2 = System.currentTimeMillis(); println(s"${k}C -> ${k}F.")
                freqItems = validateCdds(rawTrans, cdds.toSet, minCount)
                var tmpE2 = System.currentTimeMillis(); println(s"GG！ Size of ${k}F is ${freqItems.size}.Timecost:${(tmpE2-tmpS2)/1000}s\n")
                k += 1
        }
        var timee = System.currentTimeMillis()
        println(s"\nTimecost:${(timee-times)/1000}s")
        //FIs: ArrayBuffer[Array[(Set[Int], Int)]]
        val file = new File("/home/zhm/project03C/FIs.txt")
        val bw = new BufferedWriter(new FileWriter(file))
        for(vl<-FIs.flatMap(line=>line).sortBy(-_._2)){
            bw.write(vl.toString+"\n")
        }
        println(s"Write into $file")
        bw.close()
        
    }
}
