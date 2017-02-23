import scala.collection._

/*
prefix相当于某个满足最小支持度的itemA，
然后是满足与itemA组合仍大于最小支持度的itemA+itemB={itemA,itemB},
然后{itemA,itemB}扫描除了itemA、itemB的剩余项（此时这些剩余项都是包含{A,B}的），然后交集，如果仍大于最小支持度，重复上述。
假设得{A,B,C}，此时suffix为除了A\B\C三项的剩余item，这些剩余item包含了A、B、C和其余item。
{A,B,C}与剩余的itemSet交集......
—— 以上是while一次的过程。下次则是从B开始，得{B,C}，得{B,C,F}，...
*/
object eclat{
	val minSup = 0.5*6
	var p_prefix:Map[Set[Set[Int]],Int] = Map()
	def eclat(_prefix:Map[Set[Set[Int]],Int], _x:mutable.Map[Int,Set[Int]], _xx:mutable.Map[Int,Set[Int]]){
		var xx = _xx
		var prefix = _prefix
		while(!xx.isEmpty){
			var itemtid = xx.last                  //zi :Tuple2[Int, Set[Int]] //(item, TIDSet)
			var isup = itemtid._2.size
			xx = xx.dropRight(1)
			if(isup >= minSup && !(xx.keys.toSet & _x.keys.toSet).isEmpty){   //zi 取最后一个item_tid，如果满足最小支持度，并与剩余的(itemSet,TIDs)做交集。
				prefix += (prefix.keys.to[scala.collection.mutable.Set] + Set(itemtid._1) -> isup)
				println(prefix)
				var suffix:mutable.Map[Int,Set[Int]] = mutable.Map()
			    for(itremain <- xx){                                          //zi 剩余的(itemSet,TIDs)与之交集，且满足最小支持度的，递归到下一次计算。
					var tids = itemtid._2 & itremain._2
					if(tids.size > minSup){
						suffix += (itremain)
					}
				}
				eclat(prefix,_x,suffix)
			}
        }
		prefix
	}
	def main(args:Array[String]){
		var xx:mutable.Map[Int,Set[Int]] = mutable.Map(11->Set(3,4,5,6), 22->Set(1,2,3), 33->Set(4,6), 44->Set(1,3,5), 55->Set(1,2,4,5,6), 66->Set(1,2,4,6))
		var x = xx
		//var prefix:Map[Set[Int],Int] = Map()
		var result = eclat(Map(), x, xx)
		println("\n")
		println(result)

	}
}
