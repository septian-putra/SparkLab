import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document

// Check example https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala

object Twitterstats
{
    var firstTime = true
    var t0: Long = 0
    val pw = new java.io.PrintWriter(new File("tweets.txt"))
    
	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}
	
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	def getLangNameFromCode(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
    
    // Print stram to console
    // Record#. <hashTagCount> #<HashTag>:<username>:<tweetCount> < tweetText>
	def printTweets(tweets: Array[(Long, Int, String, String, Int, String)])
	{
        val seconds = (System.currentTimeMillis - t0) / 1000
        println("\n\n======================================\nTime: " + java.time.LocalTime.now
 + "\n======================================\n")
        var prevHashTag = "None"
        var count10 = 0
        var count3 = 0
        var j = 0
        
        if (seconds > 120)
        {
            while((j < tweets.size) && (count10 < 10))
            {
                val countHashTags = tweets(j)._2
                val hashTag = tweets(j)._3
                val username = tweets(j)._4
                val retweetCountcount = tweets(j)._5
                val text = tweets(j)._6
                
                if (prevHashTag != hashTag){
                    count3 = 1
                    count10 += 1
                    prevHashTag = hashTag
                    println(count10 + ". " + countHashTags + " " + hashTag + ":" + username + ":" + retweetCountcount + " " + text + "\n-----------------------------------\n")
                }else if(count3 < 3){
                    count3 += 1
                    count10 += 1
                    println(count10 + ". " + countHashTags + " " + hashTag + ":" + username + ":" + retweetCountcount + " " + text + "\n-----------------------------------\n")
                }
                j += 1
            }   
        }
	}

    // Print stram to file
    // Record#. <hashTagCount> #<HashTag>:<username>:<tweetCount> < tweetText>	
	def write2Log(tweets: Array[(Long, Int, String, String, Int, String)])	
    {
        if (firstTime)
		{
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
        
            val seconds = (System.currentTimeMillis - t0) / 1000
            
            if (seconds < 120)
            {
                println("\nElapsed time = " + seconds + " seconds. Logging will be started after 120 seconds.")
                return
            }
            
            pw.write("\n\n======================================\nTime: " + java.time.LocalTime.now + " \n======================================\n")
            
            for(i <-0 until tweets.size)
            {
                val countHashTags = tweets(i)._2
                val hashTag = tweets(i)._3
                val username = tweets(i)._4
                val retweetCountcount = tweets(i)._5
                val text = tweets(i)._6
                
                // Write to file
                pw.write((i+1) + ". " + countHashTags + " " + hashTag + ":" + username + ":" + retweetCountcount + " " + text + "\n-----------------------------------\n")
            }
        }   
    }
        
	def main(args: Array[String])
	{
		val file = new File("cred.xml")
		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file);
			
		// Configure Twitter credentials
		val consumerKey = document.getElementsByTagName("consumerKey").item(0).getTextContent 				
		val consumerSecret = document.getElementsByTagName("consumerSecret").item(0).getTextContent 		
		val accessToken = document.getElementsByTagName("accessToken").item(0).getTextContent 				
		val accessTokenSecret = document.getElementsByTagName("accessTokenSecret").item(0).getTextContent	
		
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)

		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

		val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

		val ssc = new StreamingContext(sparkConf, Seconds(2))
		val stream = TwitterUtils.createStream(ssc, None)
		
		// Insert your code here
		val tweetsStream = stream
        // Filter only tweet with count >1 (has a retweet) in English
        .filter(t => (t.isRetweet && ((getLang(t.getText) == "en") || (getLang(t.getText) == "no"))))
        // Get necessary information
        .map(status => {
            val username = status.getRetweetedStatus.getUser.getScreenName
            val text = status.getRetweetedStatus.getText
            val retweetedId = status.getRetweetedStatus.getId
            (retweetedId,(1, username, text))
        })
        .transform(rdd => rdd.sortByKey())
        // Count tweet count from the same retweetedId to get:
        // (retweetedId, (tweetCount, username, text))
        .reduceByKeyAndWindow((a:(Int, String, String), b:(Int, String, String)) => ((a._1 + b._1), a._2, a._3), Seconds(120), Seconds(2))
        
        // Cache the contents of tweetsStream
        tweetsStream.foreachRDD(rdd => rdd.cache())
        
        // Get most popular HashTag & its count for each tweet
        val PopularTagsStream = tweetsStream
        // Extract HashTag for each tweets & remove its duplicate if any to get
        // (retweetedId, List(hashTags))
        .map(x => x._1 -> x._2._3.split(" ").filter(_.startsWith("#")).map(x => if (x.last.isLetter) x else x.dropRight(1)).distinct)
        // Handle case if there is no HashTag 
        .map(x => (x._1, if (x._2.size > 0) x._2 else Array("None")))
        // Create tuple with HashTag as key and tweet Id as value:
        // (hashTags, retweetedId)
        .flatMap{case (c, innerList) => innerList.map(_ -> c)}
        // Group the tweet based on hashtag and count the HashTag :
        // ((hashTags, hashTagsCount), List(retweetedId))
        .groupByKey.mapValues(_.toList)
        // Handle tweets without hashTag
        .map{x => (x._1, if (x._1 == "None") 0 else x._2.size)-> x._2}
        // Create tuple with tweet Id as key and HashTag & Count as value :
        // (retweetedId, (hashTags, hashTagsCount))
        .flatMap {case (c, innerList) => innerList.map(_ -> c)}

        // Joining PopularTagsStream with tweetsStream using retweetedId
        val outDStream = PopularTagsStream.join(tweetsStream)
        // arrange into (retweetedId, popularHashTagsCount, popularHashTags, 
        // username, tweetCount, text)
        // tweetCount added 1 because of the original tweet.
        .map(x => (x._1, x._2._1._2, x._2._1._1, x._2._2._2, x._2._2._1+1, x._2._2._3))
        // Transform since we can't sort on DStream
        .transform(rdd =>
        // Sort the tuple by HashTagCount, retweetCount and hashTag
        rdd.sortBy(_._3).sortBy(line => (line._2, line._5), ascending=false))
        
        // Write to log file
        t0 = System.currentTimeMillis
        outDStream.foreachRDD(rdd => {
            write2Log(rdd.collect)
            printTweets(rdd.collect)
        })

		ssc.start()
		ssc.awaitTermination()
	}
}