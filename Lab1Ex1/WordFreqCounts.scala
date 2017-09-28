// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object WordFreqCounts 
{
	def main(args: Array[String]) 
	{
		val inputFile = args(0) // Get input file's name from this command line argument
		val conf = new SparkConf().setAppName("WordFreqCounts")
		val sc = new SparkContext(conf)
		
		println("Input file: " + inputFile)
		
		// Uncomment these two lines if you want to see a less verbose messages from Spark
		//Logger.getLogger("org").setLevel(Level.OFF);
		//Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
        // Load the input file creating an RDD.
        val wordFreq = sc.textFile(inputFile)
        
        // Cache the data in memory for faster, repeated retrieval.
        wordFreq.cache
        
        
        
        
        val outRDD = wordFreq
        // Insert stop token (#) for sliding and convert each line to lower case
        .map(row => ("# "+row).toLowerCase)
        // Split 2 words connected with "--" by adding stop token (#) after ("--")  
        .flatMap{case word => word.replaceAll("--","--# ").split("--")}
        // Split each line on the spaces, filter the null string and then use sliding to get succesive pair of words
        .flatMap(row => row.split(' ').filter(_.size>0).sliding(2).toList)
        // Filter the array to make sure it contains string pair
        .filter{_.size == 2}
        // Convert word pair into tuples with the second word as key. If the key contain any punctuation, add token in the end of their precedent word
        .map{case Array(b, a) => if (Character.isLetter(a.head) && (b.size > 0)) (a.replaceAll("""[\p{Punct}&&[^-]]""", ""), b) else (a.drop(1).replaceAll("""[\p{Punct}&&[^-]]""", ""), b+ "#")}
        // Clean any punctuation from the start of precedent word, but replace with token(#) if it is in the end 
        .map{case (x, y) => if (!Character.isLetter(y.last)) (x, y.dropRight(1).replaceAll("""[\p{Punct}&&[^-]]""", "") + "#") else (x, y.replaceAll("""[\p{Punct}&&[^-]]""", ""))}
        // Group by the key 
        .groupBy( _._1 ).mapValues( _.map( _._2 ))
        // Count the key and all of its precedent
        .map{x => (x._1, x._2.size, x._2.groupBy(identity).mapValues(_.size).toList)}
        // Filter the preceding word and then sort them based on the counter and alphabet
        .map{x => (x._1, x._2, x._3.filter{_._1.last.isLetter}.sortWith(_._1 < _._1).sortWith(_._2 > _._2))}
        // Sort the key words based on the counter and alphabet
        .sortBy(_._1).sortBy(_._2,false)
        // Collect the value
        .collect
        
        //Print to file
        val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outFile.txt")))
        for (x <- outRDD) {
            writer.write(x._1 + ":"+ x._2)
            for (y <- x._3){
                writer.write("\r\n\t"+ y._1 + ":"+ y._2)
            }
            writer.write("\r\n")
        }
        writer.close()
        
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}
