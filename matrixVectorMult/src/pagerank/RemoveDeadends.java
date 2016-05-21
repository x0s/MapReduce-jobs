package pagerank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RemoveDeadends {

	enum myCounters{
		NUMNODES;
	}

	private static int cpt = 0;

	private final static String TYPE_PRED = "P"; //-
	private final static String TYPE_SUCC = "S"; //-

	static class Map extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				// On récupère les prédécesseurs et successeurs:
				String[] values  = value.toString().split("\\s+"); //"j i" -> (j,i)
				Text predecesseur = new Text(values[0]);
				Text successeur = new Text(values[1]);

				// On renvoie les relations des deux points de vue (j a tel successeur et tel précédesseur)
				// Cela nous permettra de détecter des deadends
				context.write(predecesseur, new Text(successeur + " " + TYPE_SUCC)); //(K,V) = (j, "i S")
				context.write(successeur, new Text(predecesseur + " " + TYPE_PRED)); //(K,V) = (i, "j P")
			}
		}


	static class Reduce extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			ArrayList<String> listPredecessors = new ArrayList<String> ();
			ArrayList<String> listSuccessors = new ArrayList<String> ();

			for (Text value : values)
			{
				String[] splits  = value.toString().split("\\s+"); // "j P" -> (j,"P") et "i S" -> (i,"S")

				//Pour un noeud donné j, on stocke dans deux listes les prédécesseurs et les successeurs
				if(splits[1].equalsIgnoreCase(TYPE_PRED))
				{
					listPredecessors.add(splits[0]);
				}
				else if(splits[1].equalsIgnoreCase(TYPE_SUCC))
				{
					listSuccessors.add(splits[0]);
				}

			}

			//On renvoie les nodes qui ne sont pas des deadends
			if (!listSuccessors.isEmpty()) { 		// Si cette node n'est pas une deadend de Type I  (No successors)
				if (!(listPredecessors.size() == 1 	// Si cette node n'est pas une deadend de Type II (self-Successes and self-precedes)
						&& listSuccessors.size()==1
						&& listPredecessors.get(0).equalsIgnoreCase(key.toString()))) {
					//ON incrémente le compteur
					Counter c = context.getCounter(myCounters.NUMNODES);
					c.increment(1);
					for (String pred : listPredecessors)
					{
						//On renvoie les relations pas détectées comme deadends (une fois par relation dans le sens j->i)
						context.write(new Text(pred), key); // (K,V) = (j, i) =(pred, succ)
					}
				}
			}

		}
}

	public static void job(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{

		boolean existDeadends = true;

		boolean firstLoop = true;

		/* 
		 * In the beginning, the initial graph is copied in the processedGraph. After this, the working directories are processedGraphPath and intermediaryResultPath.
		 * The final output should be in processedGraphPath.
		 */
		FileUtils.copyDirectory(new File(conf.get("graphPath")), new File(conf.get("processedGraphPath")));
		String intermediaryDir = conf.get("intermediaryResultPath");
		String currentInput = conf.get("processedGraphPath");

		// nNodes compte le nombres de nodes détectées comme "non deadends" à la sortie du MapReduce(n)
		long nNodes = conf.getLong("numNodes", 0);

		Job job;
		Counters counters;
		Counter c;

		while(existDeadends)
		{
			// S'il reste des deadends après le premier MapReduce, on en relance un autre jusqu'à ce qu'il n'y ait plus de deadends
			// On remplace les données d'input par celles de l'output du précédent MapReduce
			if(firstLoop == false)
			{
				//Suppression de l'input de du MapReduce(n-1)
				FileUtils.deleteDirectory(new File(currentInput));
				//On copie l'output du MapReduce(n-1) dans l'input du MapReduce(n)
				FileUtils.copyDirectory(new File(intermediaryDir), new File(currentInput));
				//On supprime le dossier d'output du MapReduce(n-1) pour éviter la collision des fichiers
				FileUtils.deleteDirectory(new File(intermediaryDir));
			}
			job = Job.getInstance(conf);
			job.setJobName("deadends job");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);//splits a block of data into lines and sends to the map function one line at the time
			job.setOutputFormatClass(TextOutputFormat.class);//converts the key and value to strings and writes them on one line: string(key) separator string(value)

			FileInputFormat.setInputPaths(job, new Path(currentInput));
			FileOutputFormat.setOutputPath(job, new Path(intermediaryDir));
			job.waitForCompletion(true);

			counters = job.getCounters();
			c = counters.findCounter(myCounters.NUMNODES);

			// Si le nombre de node "non deadends" n'a pas bougé d'un MapReduce(n-1) au MapReduce(n), c'est qu'il n'y en a plus
			if (c.getValue() == nNodes) { //Si nNodes(n)=nNodes(n-1)
				existDeadends = false;

			} else {
				nNodes = c.getValue();
				conf.setLong("numNodes", nNodes);
			}

			firstLoop = false;

		}

	}

}
