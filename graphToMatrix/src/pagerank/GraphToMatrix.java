package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GraphToMatrix {

	static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		/*
		 * (key, value)
		 * Key: Adresse de la ligne du fichier d'input splitté
		 * value: String contenant la ligne du fichier à traiter
		 * context: Sortie du mapper dans laquelle écrire les couples de valeurs (sort)
		 */
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			//On récupère les indices de pages du graphes "j i" -> (j,i)
			String[] values  = value.toString().split("\\s+"); // "\s" caractère espace
			IntWritable outputKey = new IntWritable(Integer.parseInt(values[0]));
			IntWritable outputValue = new IntWritable(Integer.parseInt(values[1]));
			System.out.println("page j:" + values[1] + "page i" + values[0]);
			context.write(outputKey, outputValue); // On envoie les valeurs sorted (extraites)
		}
	}

	static class Reduce extends Reducer<IntWritable, IntWritable, NullWritable, Text> {

		/*
		 * (key, value)
		 * Key:  page j parente contenant les liens hypertextes vers df'autres pages i (les successeurs)
		 * Value: Iterable tableau d'entiers contenant les numéros des pages vers lequel pointe la page j
		 */
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

			NullWritable returnKey = null;

			//On va itérer au travers de l'iterable values contenant les successeurs d'une page j
			ArrayList<IntWritable> listeDesSuccesseurs = new ArrayList<IntWritable> ();
			for (IntWritable value : values)
			{
				listeDesSuccesseurs.add(new IntWritable(value.get()));
			}
			for( IntWritable unSuccesseur: listeDesSuccesseurs)
			{
				//on va construire la ligne à écrire dans l'output avec le format "i j Mij"
				String Mij = Double.toString(1.0/listeDesSuccesseurs.size());

				String ligneOutput = unSuccesseur.toString() + " " + key.toString() + " " + Mij;

				context.write(returnKey, new Text(ligneOutput));
			}

		}
	}

	public static void job(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get("processedGraphPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("stochasticMatrixPath")));
		job.waitForCompletion(true);
	}

}
