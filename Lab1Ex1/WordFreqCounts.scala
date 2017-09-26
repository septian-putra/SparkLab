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
        // Convert each line to lower case
        .map(line => line.toLowerCase)
        // Insert stop token () for sliding
        .map(word => ("# "+word))
        // Split each line on the spaces and then use sliding to get succesive pair of words
        .flatMap(row => row.split(' ').sliding(2).toList)
        // Convert word pair into tuples with the second word as key. If the key contain any punctuation, add token in the end of their precedent word
        .map{case Array(b, a) => if (!Character.isLetter(a.head)) (a.replaceAll("""[\p{Punct}&&[^-]]""", ""), b + "#") else (a.replaceAll("""[\p{Punct}&&[^-]]""", ""), b)}
        // Clean any punctuation from the start of precedent word, but replace with token(#) if it is in the end 
        .map{case (x, y) => if (!Character.isLetter(y.last)) (x, y.replaceAll("""[\p{Punct}&&[^-]]""", "") + "#") else (x, y.replaceAll("""[\p{Punct}&&[^-]]""", ""))}
        // Group by the key 
        .groupBy( _._1 ).mapValues( _.map( _._2 ))
        // Count the key and all of its precedent
        .map{x => (x._1, x._2.size, x._2.groupBy(identity).mapValues(_.size).toList)}
        // Sort the element based on the counter
        .map{x => (x._1, x._2, x._3.sortWith(_._2 > _._2))}.sortBy(_._2,false)
        // Collect the value
        .collect
        
        //Print to file
        val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outFile.txt")))
        for (x <- outRDD1) {
          writer.write(x._1 + ":"+ x._2)
          for (y <- x._3){
            if (Character.isLetter(y._1.last))
                writer.write("\r\n\t"+ y._1 + ":"+ y._2)
          }
          writer.write("\r\n")
        }
        writer.close()
        
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}
