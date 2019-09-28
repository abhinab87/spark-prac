import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

import static java.io.FileDescriptor.out;

/**
 * Created by abhin on 11/20/2017.
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WorCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData;
        distData = sc.parallelize(data);
        //JavaRDD<String> distFile = sc.textFile("data.txt");
        JavaRDD<String> lines = sc.textFile("C:/Users/abhin/Desktop/PersonalInfo.txt");
        JavaRDD<Integer> wordsLenth =
                lines.map(line -> line.length());
            wordsLenth.collect().forEach(x -> System.out.println(x));
        //int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(wordsLenth);
    }
}
