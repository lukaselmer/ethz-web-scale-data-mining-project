import java.util.Comparator;

import scala.Tuple2;
//Comparator.

public class TupleComparator implements Comparator<Tuple2<String, Integer>>
{
    @Override
        public int compare(Tuple2<String, Integer> x, Tuple2<String, Integer> y) {
            if (x._2() > y._2())
            {
                return 1;
            }
            /*
            if (x._2() > y._2())
            {
                return -1;
            }
            */
            return 0;
        }
}


