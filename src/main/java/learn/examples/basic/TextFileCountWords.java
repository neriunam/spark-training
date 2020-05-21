package learn.examples.basic;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class TextFileCountWords {
	
	public static void main(String[] args) {
		
		log.info("Start word counts");
		
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		String mdFilePath = Paths.get(prop.getProperty(Constants.HOME_SPARK_2_3), "README.md").toString();
		
		JavaSparkContext sc = new JavaSparkContext("local", "appname");
		
		JavaRDD<String> mdLinesFile = sc.textFile(mdFilePath);
		
		JavaRDD<String> words = mdLinesFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		
		JavaPairRDD<String, Integer> wordPairs = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		
		JavaPairRDD<String, Integer> countWords = wordPairs.reduceByKey((x, y) -> x + y);
		
		countWords.sortByKey();
		countWords.collect().forEach(System.out::println);
		
		
		sc.close();
		log.info("End");
	}
	
}
