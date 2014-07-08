/**
 * Created by root on 7/8/14.
 */
import java.util.*;
public class Cache {
    public static Map<Double,Double> lruCache(final int maxSize) {
        return new LinkedHashMap<Double, Double>(maxSize*4/3, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Double,Double> eldest) {
                return size() > maxSize;
            }
        };
    }
}

