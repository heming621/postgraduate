import scala.collection._
import scala.io.Source
import java.io._
/*
小集合_x，大集合xx，类似分区_x，广播变量xx；
输入：_x、xx，输出：只出现在_x里面的频繁项集。
————————
关于添加的“(_x.keys.toSet.contains(itemtid._1) | !prefix.isEmpty)”，
'|'前的条件表明：只有出现在小集合_x里面的item才能继续，但是这样会导致递归的情况没有纳入；所以在'|'后把递归的情况纳入。
'|'前的条件将(出现在_x里的)一项频繁集搞定；'|'和后面的if将(出现在_x里的)2+项频繁集搞定；
递归的时候，prefix不为空，
但递归的时候itemtid的值是xx.last获取，单值，不含prefix(若包含prefix值通过与_x交集就能判断 是否在_x)，所以'|'前条件不能过滤，会漏掉二项及以上项频繁集。
*/
object eclat{
	val minSup = 0.5*6
	def eclat(_prefix:Set[Int], _x:Map[Int,Set[Int]], _xx:mutable.Map[Int,Set[Int]], result:mutable.Map[Set[Int], Int]):mutable.Map[Set[Int],Int] = {//:Unit = {
		var xx = _xx
		var prefix = _prefix
		while(!xx.isEmpty){
			var itemtid = xx.last           
			var isup = itemtid._2.size
            var prefixRcs:Set[Int] = Set()  
			xx = xx.dropRight(1)            
			if(isup >= minSup && (_x.keys.toSet.contains(itemtid._1) | !prefix.isEmpty)){    // //zi 只对出现在_x里面的item生成频繁集。//zi 取最后一个item_tid，如果满足最小支持度，并与剩余的(itemSet,TIDs)做交集。
                prefixRcs = prefix + itemtid._1
                if(prefix.isEmpty){
                    result += Set(itemtid._1) -> isup
                }
                else{
                    println(s"prefix:${prefix}")
                    result += prefixRcs -> isup
                }
                println(s"${prefixRcs -> isup}\n") 
                var suffix:mutable.Map[Int,Set[Int]] = mutable.Map()
				for(itremain <- xx){                             //zi 剩余的(itemSet,TIDs)与之交集，且满足最小支持度的，递归到下一次计算。
					var tids = itemtid._2 & itremain._2
					if(tids.size >= minSup){
						suffix += itremain._1 -> tids    //zi 剩余项与itemA交集，仍大于支持度的；留下，成为新剩余项(item,TIDS)->(item,TIDs')。TID数目改变，决于与A相交的事务个数。
						                                 //zi 假设itemB与A的事务交集大于minSup，则递归时，看似存项B的支持度，实际存的是(A,B)项集的支持度。
					}                                            
				}
				if(!(_x.keys.toSet & prefixRcs).isEmpty)     //zi 前缀prefix在小集合_x里，出现过的才继续递归，继续生成维度更高的项集。
				    eclat(prefixRcs, _x, suffix, result) 
			}
		}
	    result	
	}
	def main(args:Array[String]){
        		var xx:mutable.Map[Int,Set[Int]] = mutable.Map(11->Set(3,4,5,6), 22->Set(1,2,3), 33->Set(4,6), 44->Set(1,3,5), 55->Set(1,2,4,5,6), 66->Set(1,2,4,6))
		        //var x:mutable.Map[Int,Set[Int]] = mutable.Map(66->Set(1,2,4,6), 11->Set(3,4,5,6))//mutable.Map(11->Set(3,4,5,6), 55->Set(1,2,4,5,6))//
		        var x = xx
                var results:mutable.Map[Set[Int],Int] = mutable.Map()
        		eclat(Set(), x.toList.toMap, xx, results)
        		println("\n")
        		println(results)
                println(results.size)

	}
}



/*
1       22、    44、55、66
2       22、        55、66
3   11、22、    44
4   11、    33、    55、66
5   11、        44、55
6   11、    33、    55、66
*/
