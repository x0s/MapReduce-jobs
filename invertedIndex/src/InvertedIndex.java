import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class InvertedIndex {
	private static final Pattern alphaDigit = Pattern
			.compile("[\\p{L}\\p{N}]+");

	// In a real application, this would be an Apache Zookeeper shared counter
	static int corpusSize 	 		= 0; // number of documents in a corpus
	static Stemmer stemmer 	 		= new Stemmer();
	static Set<String> stopWordsSet = new HashSet<String>();
	static int decimalPrecision		= 5; //number of decimals for TFIDF calculation
	
	/**
	 * get the number of documents currently indexed 
	 */
	public static int getCorpusSize() {
		return corpusSize;
	}

	static class Map extends Mapper<Text, Text, Text, PairTextDoubleWritable> {
		/**
		 * Mapper: Extract words counts not stopwords
		 * Input  : (Key, Value) = (document title	, document content			  )
		 * Output : (Key, Value) = (term			, (document title, TF weight) )
		 */
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			int wordCountTotal 					= 0; 									// Non stop words count in the current document
			Matcher m 							= alphaDigit.matcher(value.toString());
			TreeMap<String, Integer> wordCounts	= new TreeMap<String,Integer>();
			while (m.find()) {
				String token = m.group();
		
				// non-StopWords normalization(stemming, lowercasing)
				String token_stemmed = stemmer.stem(token).toLowerCase();
				if(!isStopWord(token_stemmed)) {
					wordCountTotal++;
					// Get current wordCount and update the hashmap
					//int currentWordCount = (wordCounts.containsKey(token_stemmed)) ? wordCounts.get(token_stemmed) : 0;
					int currentWordCount = (null == wordCounts.get(token_stemmed)) ? wordCounts.get(token_stemmed) : 0;
					wordCounts.put(token_stemmed, currentWordCount+1);
				}
			}

			// Map write: TF + write in context
			for(Entry<String, Integer> wordCount : wordCounts.entrySet()) {
				double TF = ((double)wordCount.getValue()) / wordCountTotal;
				context.write(new Text(wordCount.getKey()), new PairTextDoubleWritable(key, new DoubleWritable(TF)));
			}
			corpusSize++;
		}
	}

	static class Reduce extends
			Reducer<Text, PairTextDoubleWritable, Text, Text> {
		/**
		 * As we need to exhaust twice the iterable to get both the size and the elements, we have to store it.
		 * Input:  PairTextDoubleWritable(< Text	, DoubleWritable >)
		 * Output: HashMap				 (< String	, Double	     >) = (Document title, TF)
		 */
		private HashMap<String, Double> getDocumentsTFHashMap(Iterable<PairTextDoubleWritable> values) {

			HashMap<String, Double> toHashMap = new HashMap<String, Double>();
			
			for(PairTextDoubleWritable pair: values) {
				toHashMap.put(
						pair.getFirst().toString(), 
						pair.getSecond().get()
						);
			}
			
			return toHashMap;
		}
		/**
		 * 	Input:  (Key, Value) = (term, iterable<document title, TF>)
		 *  Output: (Key, Value) = (term, Text("documentTitle1:tfidf1\tdocumentTitle2:tfidf2\t..."))
		 */
		@Override
		protected void reduce(Text key,
				Iterable<PairTextDoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			// Convert exhaustable Iterable to reIterable HashMap
			HashMap<String, Double> documentTFMap = getDocumentsTFHashMap(values);
			
			// IDF calculation: IDF = N_documents / N_documents where term occurs
			double totalDocumentsWithWord = documentTFMap.size();
			double IDF = Math.log(  ((double) getCorpusSize()) / totalDocumentsWithWord) / Math.log(2);
			
			TreeSet<DocumentWeight> documentTfidfSet = new TreeSet<DocumentWeight>(); //Ordered Set
			
			// TFIDF calculation for each document
			for(Entry<String, Double> documentsTF: documentTFMap.entrySet()) {
				double TFIDF =  round( documentsTF.getValue() * IDF, decimalPrecision);
				//Add (document title, TFIDF)
				documentTfidfSet.add(new DocumentWeight(documentsTF.getKey(), TFIDF));
			}
			
			//Formating output
			String postingListSorted = "";
			String tabString 		 = "";
			for(DocumentWeight documentTfidf: documentTfidfSet) {
				postingListSorted = postingListSorted + tabString + documentTfidf.documentTitle + ":" + String.valueOf(documentTfidf.weight);
				tabString 		  = "\t";
			}
		
			context.write(key, new Text(postingListSorted));
		}
	}
	/**
	 * Loads the stop word list referenced in the filename string (one stopword per line)
	 * into an in-memory structure (hashSet)
	 * stop words are stemmed to be compared with stemmed tokens
	 * @param filename
	 * @throws IOException
	 */
	public static void retrieveStopWords(String filename) throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		    String stopWord;
		    while ((stopWord = br.readLine()) != null) {
		       stopWordsSet.add(stemmer.stem(stopWord));
		    }
		}
	}
	
	/**
	 * Check if a word is a stop word
	 * @param token 	word to test
	 * @return boolean
	 */
	public static boolean isStopWord(String token) {
		return stopWordsSet.contains(token);
	}

	public static void buildInvertedIndex(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PairTextDoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(false);
	}
	
	public static class DocumentWeight implements Comparable<DocumentWeight>
	{
		String documentTitle;
		double weight;
		
		DocumentWeight(String t, double d)
		{
			documentTitle=t;
			weight=d;
		}
		
		@Override
		public int compareTo(DocumentWeight that) {
			// DONE: compareTo
			return (weight < that.weight) ? 1 : -1;
		}
	}
	/**
     * Round a double to certain number of decimals
     * 
     * @param d				double to round
     * @param decimalPlace	number of decimals
     * @return				the rounded double
     */
    public static Double round(Double d, int decimalPlace) {
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }
}
