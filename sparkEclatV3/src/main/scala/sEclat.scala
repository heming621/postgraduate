import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark._
import scala.collection.mutable.Set
import scala.collection._
import scala.io.Source
import java.io._

object sEclat{
	def eclat(_prefix:Set[Int], _x:Map[Int,Set[Int]], _xx:mutable.Map[Int,Set[Int]], result:mutable.Map[Set[Int], Int], minSup:Int):Iterator[mutable.Map[Set[Int],Int]] = {
		var xx = _xx
		var prefix = _prefix
		while(!xx.isEmpty){
			var itemtid = xx.last           
			var isup = itemtid._2.size
			var prefixRcs:Set[Int] = Set()  
			xx = xx.dropRight(1)            
			if(isup >= minSup && (_x.keys.toSet.contains(itemtid._1) | !prefix.isEmpty)){    //zi 只对出现在_x里面的item生成频繁集。//zi 取最后一个item_tid，如果满足最小支持度，并与剩余的(itemSet,TIDs)做交集。
                prefixRcs = prefix + itemtid._1
				//println(prefixRcs -> isup)    
                if(prefix.isEmpty)                               //zi prefix若为空，则非递归。
					result += Set(itemtid._1) -> isup
                else
                    result += prefixRcs -> isup
                var suffix:mutable.Map[Int,Set[Int]] = mutable.Map()
				for(itremain <- xx){                             //zi 剩余的(itemSet,TIDs)与之交集，且满足最小支持度的，递归到下一次计算。
					var tids = itemtid._2 & itremain._2
					if(tids.size >= minSup){
						suffix += itremain._1 -> tids    //zi 剩余项与itemA交集，仍大于支持度的；留下，成为新剩余项(item,TIDS)->(item,TIDs')。TID数目改变，决于与A相交的事务个数。
						                                 //zi 假设itemB与A的事务交集大于minSup，则递归时，看似存项B的支持度，实际存的是(A,B)项集的支持度。//zi 递归时候prefix不空。
					}                                            
				}
				if(!(_x.keys.toSet & prefixRcs).isEmpty && prefixRcs.size < 7)
					eclat(prefixRcs, _x, suffix, result, minSup) 
			}
		}
	    Iterator(result)
	}

	def main(args: Array[String]){
		var stime = System.currentTimeMillis()
		val HDATA_PATH = "hdfs://zhm01:9000/user/root/data/mushroom.dat"
		val LDATA_PATH = "/home/mountDir/downloads/data/kosarak.dat"//"/home/mountDir/downloads/data/T40I10D100K.dat"
		val minSup = (0.02*990002).toInt//(0.1*8124).toInt
		val conf = new SparkConf().setAppName("sEclat")
		                          .set("spark.master", "spark://zhm01:7077")
								  .set("spark.driver.memory", "28g") //
								  .set("spark.executor.memory", "18g")
								  .set("spark.executor.cores","8")
								  .set("spark.cores.max", "80")
								  .set("spark.default.parallelism", "40")
        val sc = new SparkContext(conf)
        ////// READ FILE and get itemSet_TIDs, store in itemTid.
		var tid = 0
		var itemTid:scala.collection.mutable.Map[Int, Set[Int]] = scala.collection.mutable.Map()
		val bufferedSource = Source.fromFile(LDATA_PATH)
        for(line<-bufferedSource.getLines){
			tid += 1;
			for(item<-line.split(" ")){
				var itemi = item.toInt
				if(!itemTid.contains(itemi))
					itemTid += itemi->Set(tid)
				else
					itemTid(itemi) += tid
			}
		}
		bufferedSource.close()
		////// test
		//val file = new File("/home/zhm/sparkEclatV1/test.txt")
        //val bw = new BufferedWriter(new FileWriter(file))
		//for(vl<-itemTid)bw.write(vl.toString+"\n")
		//bw.close()
		//// broadcast itemSet_TIDs and FIM
		//itemTid = mutable.Map(11->Set(3,4,5,6), 22->Set(1,2,3), 33->Set(4,6), 44->Set(1,3,5), 55->Set(1,2,4,5,6), 66->Set(1,2,4,6))
		//zi 因mapPartitions表示元素为分区，所以后面需要flatMap或类flatten展开。
		var bcItemTid = sc.broadcast(itemTid)
		var results:mutable.Map[Set[Int],Int] = mutable.Map()
		var results2 = sc.parallelize(itemTid.toList).filter(x => x._2.size >= minSup).mapPartitions(part=>eclat(Set(), part.toMap, bcItemTid.value, results, minSup)).flatMap(x=>x).collect()
		//var results2 = sc.parallelize(itemTid.toList).mapPartitions(part=>eclat(Set(), part.toMap, bcItemTid.value, results, minSup)).flatMap(x=>x).collect()
		bcItemTid.unpersist
		bcItemTid.destroy
        
		//// WRITE
        val fileName = new File("/home/zhm/sparkEclatV3/test.txt")
		val bw = new BufferedWriter(new FileWriter(fileName))
		var resultSort = results2.sortBy(_._2)
		for(ve<-resultSort)
			bw.write(ve.toString+"\n") //for(ve<-results2)println(ve)
		bw.close()
		var etime = System.currentTimeMillis()
		println(s"Timecost: ${(etime-stime)/1000}s.")
		println(s"Write into $fileName")
		println(s"ResultsSize: ${results2.size}")

	}
}








