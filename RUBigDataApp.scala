package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import sys.process._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.{URL, URI}
import java.io.File

object AnalyzerApp {
	def main(args: Array[String]) {
    	val spark = SparkSession.builder.appName("SentenceAnalyzer").getOrCreate()
		val sc = spark.sparkContext
		val fs = FileSystem.get(new URI("hdfs:/"), sc.hadoopConfiguration)
    		val baseDir = s"hdfs:/user/s4608682/"
		val baseURL = s"https://commoncrawl.s3.amazonaws.com/"
		val extension = s".warc.gz"

		val outlet_one = "theSun"
		val sunURI = s"crawl-data/CC-MAIN-2020-40/segments/1600400196999.30/wet/CC-MAIN-20200920062737-20200920092737-00419.warc.wet.gz"
		val sunDomain = "thesun.co.uk"
		
		val outlet_two = "washingtonPost"
		val washURI = s"crawl-data/CC-MAIN-2017-04/segments/1484560280410.21/wet/CC-MAIN-20170116095120-00264-ip-10-171-10-70.ec2.internal.warc.wet.gz"
		val washDomain = "washingtonpost.com"

		val outlet_three = "FoxNews"
		val foxURI = s"crawl-data/CC-MAIN-2020-40/segments/1600400188841.7/wet/CC-MAIN-20200918190514-20200918220514-00609.warc.wet.gz"
		val foxDomain = "foxnews.com"

		val outlet_four = "theDailyMirror"
		val dailyURI = s"crawl-data/CC-MAIN-2020-34/segments/1596439739048.46/wet/CC-MAIN-20200813161908-20200813191908-00191.warc.wet.gz"
		val dailyDomain = "mirror.co.uk"
		
		val adjectives_path = s"hdfs:/user/s4608682/adjectives.txt"
		val adverbs_path = s"hdfs:/user/s4608682/adverbs.txt"
		val adjectives_RDD = sc.textFile(adjectives_path).map{x => x.toLowerCase()}
		val adverbs_RDD = sc.textFile(adverbs_path).map{x => x.toLowerCase()}

		//Collect the data back in the driver application, then broadcast it
		val adjectives = sc.broadcast(adjectives_RDD.collect().toList)
		val adverbs = sc.broadcast(adverbs_RDD.collect().toList)

		DownloadWarc(fs, outlet_one+extension, baseDir, baseURL+sunURI)
		ProcessWarc(sc, outlet_one+extension, sunDomain, baseDir, 1000, 3, adjectives, adverbs)

		DownloadWarc(fs, outlet_two+extension, baseDir, baseURL+washURI)
		ProcessWarc(sc, outlet_two+extension, washDomain, baseDir, 1000, 3, adjectives, adverbs)

		DownloadWarc(fs, outlet_three+extension, baseDir, baseURL+foxURI)
		ProcessWarc(sc, outlet_three+extension, foxDomain, baseDir, 1000, 3, adjectives, adverbs)

		DownloadWarc(fs, outlet_four+extension, baseDir, baseURL+dailyURI)
		ProcessWarc(sc, outlet_four+extension, dailyDomain, baseDir, 1000, 3, adjectives, adverbs)

	    	spark.stop()
	}

	//Allows us to interpret bool as int (used in aggregation)
	implicit def boolAsInt(bool:Boolean) = if (bool) 1 else 0

	def DownloadWarc(fs : org.apache.hadoop.fs.FileSystem, filename : String, hdfsBase : String, cURL : String) {
		if(!fs.exists(new Path(hdfsBase + filename))){
        	(new URL(cURL) #> new File(filename)).!
        	val local = new Path(filename)
        	val remote = new Path(hdfsBase+filename)
        
        	fs.copyFromLocalFile(true, true, local,remote)
    	}
	}

	def ProcessWarc(sc : org.apache.spark.SparkContext, filename : String, domain : String, hdfsBase : String, maxSentences : Int, sentenceEpsilon : Int, adjectives : org.apache.spark.broadcast.Broadcast[List[String]], adverbs : org.apache.spark.broadcast.Broadcast[List[String]]) {
    
    	//Step 1: parse the actual file to an RDD of records
    	val warcs = sc.newAPIHadoopFile(
                  	hdfsBase + filename,
                  	classOf[WarcGzInputFormat],
                  	classOf[NullWritable],        
			classOf[WarcWritable]
        	).map{_._2}
        	.filter{_.isValid()}
        	.map{_.getRecord()}
        	.cache()
    
    	//Step 2: filter the raw content from the .wet file
    	val rawcontent = warcs.filter{record => record.getHeader().getUrl() != null && record.getHeader().getUrl().contains(domain)}.map{x => {
        	var b = ""
        	for(a <- 0 to x.getHeader().getContentLength().toInt){b += x.getRecord().read().toChar}
        	b
    	}}

    	//Step 3: parse sentences
    	val sentences = rawcontent.map{cnt => {
        	val sentenceRegex = raw"""[\n\r\t" "]*([^\n\r\.]+?[\.\!\?])""".r
        	val sentenceIterator = sentenceRegex.findAllIn(cnt)
        	val fragmented = new ListBuffer[String]()
        	while(sentenceIterator.hasNext){
            	val fragment = sentenceIterator.next
            	fragmented += sentenceIterator.group(1)
            	}
        	fragmented.toList
        	}
    	}.flatMap(identity).take(maxSentences)
    
    	//Step 4: parse words, subclauses and all other fancy stuff
    	val sentenceStats = sentences.map{snt => {
        	val sentenceRegex = raw"""([a-zA-Z]+)""".r
        	val clauseRegex = raw"""[^,]*(?<=[,])[" "]*([^,]+)[" "]*[,]""".r
        	val sentenceIterator = sentenceRegex.findAllIn(snt)
        	val clauseIterator = clauseRegex.findAllIn(snt)
        	var counter, totalWLength , adjectiveCount, adverbCount, clauseCount : Int = 0
        	var question = snt.endsWith("?")
        	var exclamation = snt.endsWith("!")
        
        	//This would've been nicer with a conventional for loop. IF SCALA HAD ONE
        	while(clauseIterator.hasNext){
            	clauseIterator.next
            	clauseCount += 1
        	}
        
        	while(sentenceIterator.hasNext){
            	val word = sentenceIterator.next.toLowerCase()
            	totalWLength += word.length()
            	counter += 1
            	for(candidate <- adjectives.value){
                	if(word == candidate)
                    	adjectiveCount += 1
                	    }
            	for(secondCandidate <- adverbs.value){
                	if(word == secondCandidate)
                	    adverbCount += 1
            	        }
        	    }
        	var totalCount : Float = (adverbCount+adjectiveCount).toFloat/counter.toFloat
	        (counter > sentenceEpsilon, counter, totalWLength/counter.toFloat, clauseCount, question:Int, exclamation:Int, adverbCount/counter.toFloat, adjectiveCount/counter.toFloat, totalCount)
    	}}.filter{_._1} //Filter out only the sentences we consider "valid" (initially formulated this as "long enough", but the densitiy of gay jokes would've gotten too high)
    
    	//Step 5: aggregate data
    	val entries : Float = sentenceStats.length
    	val zeroElement = (true, 0, 0.0f, 0, 0, 0, 0.0f, 0.0f, 0.0f)
    	//Well-endowed function signature
    	def combine = {(x:(Boolean, Int, Float, Int, Int, Int, Float, Float, Float), y:(Boolean, Int, Float, Int, Int, Int, Float, Float, Float)) => (true, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8, x._9 + y._9)}
    	//Aggregate should be distributed-data-friendly
    	val foldedStats = sentenceStats.aggregate(zeroElement)(combine, combine)
    
    	//Step 6: print stats
    	println("Results for domain "+domain)
    	println("Average words per sentence: " + foldedStats._2/entries)
    	println("Average word length: " + foldedStats._3/entries)
    	println("Clause to sentence ratio: " + foldedStats._4/entries)
    	println("Question to sentence ratio: " + foldedStats._5/entries)
    	println("Exclamations to sentence ratio: " + foldedStats._6/entries)
    	println("Average adverb ratio to words: " + foldedStats._7/entries)
    	println("Average adjective ratio to words: " + foldedStats._8/entries)
    	println("Average adverb or adjective ratio to words: " + foldedStats._9/entries)
	}
}
