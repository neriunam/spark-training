package learn.examples.basic;

import java.nio.file.Paths;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * Ejemplo que imprime las prineras N lineas de un archivo de texto<br>
 * Se utiliza la clase JavaSparkContext
 * @author neri
 *
 */
@Slf4j
public class FileTextPrintNLines {
	
	private static int NUM_LINES = 10;
	
	public static void main(String[] args) {
		
		log.info(">>>>>>>>>>Start");
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		JavaSparkContext ctx = new JavaSparkContext("local", "Ejemplo N Lineas archivo de texto");
		
		String filePath = Paths.get(prop.getProperty(Constants.HOME_SPARK_2_3), "README.md").toString();
		JavaRDD<String> rdd = ctx.textFile(filePath);
		
		// Se imprimen las primeras NUM_LINES del archivo
		rdd.take(NUM_LINES).forEach(log::info);
		
		ctx.close();
		log.info(">>>>>>>>>>End");
	}
	
}
