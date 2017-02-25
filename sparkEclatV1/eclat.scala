import scala.collection._

/*
prefix相当于某个满足最小支持度的itemA，
然后是满足与itemA组合仍大于最小支持度的itemA+itemB={itemA,itemB},这时
suffix存储的是(itemB,TIDs')，其中TID'也含itemA对应的tid；
递归的时候，取itemB的TID个数，其实就相当于取itemA&itemB的TID个数。存入prefix，看似存(itemB, SUP)，其实是存(itemA&itemB, SUP)
然后与剩下的(item,TIDs)取交集，将满足与{A,B}的事务数交集仍大于minSup的，做如上处理。
—— 以上是while一次的过程。下次则是从B开始，得{B,C}，得{B,C,F}，...
*/
object eclat{
	val minSup = 0.5*6
	var p_prefix:Map[Set[Int],Int] = Map()
	def eclat(_prefix:Map[Set[Int],Int], _x:mutable.Map[Int,Set[Int]], _xx:mutable.Map[Int,Set[Int]]){
		var xx = _xx
		var prefix = _prefix
		var count:Int = 0
		while(!xx.isEmpty){
			count += 1; println(s"Count:${count}");//println(s"prefix: ${prefix}; \nsuffix: ${xx}")
			var itemtid = xx.last                  //zi :Tuple2[Int, Set[Int]] //(item, TIDSet)
			var isup = itemtid._2.size
			println(s"itemtid:${itemtid}: ${isup}; ${prefix}")
			xx = xx.dropRight(1)                   //zi && !(xx.keys.toSet & _x.keys.toSet).isEmpty
			if(isup >= minSup){   //zi 取最后一个item_tid，如果满足最小支持度，并与剩余的(itemSet,TIDs)做交集。
				prefix += (prefix.keys.to[scala.collection.mutable.Set].flatten + itemtid._1) -> isup
				println(prefix)
				var suffix:mutable.Map[Int,Set[Int]] = mutable.Map()
				for(itremain <- xx){                             //zi 剩余的(itemSet,TIDs)与之交集，且满足最小支持度的，递归到下一次计算。
					var tids = itemtid._2 & itremain._2
					if(tids.size >= minSup){
						suffix += itremain._1 -> tids            //zi 剩余项与itemA交集，仍大于支持度的；留下，成为新剩余项(item,TIDS)->(item,TIDs')。TID数目改变，决于与A相交的事务个数。
						                                         //zi 假设itemB与A的事务交集大于minSup，则递归时，看似存项B的支持度，实际存的是(A,B)项集的支持度。
					}                                            
				}
				println(s"prefix:${prefix}; suffix:$suffix;\n")
				//println(s"suffix:${suffix}\n")
				eclat(prefix, _x, suffix)
			}
		}
		//prefix
	}
	def main(args:Array[String]){
		var xx:mutable.Map[Int,Set[Int]] = mutable.Map(11->Set(3,4,5,6), 22->Set(1,2,3), 33->Set(4,6), 44->Set(1,3,5), 55->Set(1,2,4,5,6), 66->Set(1,2,4,6))
		var x = xx
		//var prefix:Map[Set[Int],Int] = Map()
		//var tvl:mutable.Map[Int,Set[Int]] = mutable.Map()
		//var result = eclat(Map(), x, tvl)
		var result = eclat(Map(), x, xx)
		println("\n")
		println(result)

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
