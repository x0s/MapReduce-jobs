package pagerank;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixVectorMult {
	/*
	 * Input (key, value): (Long, Text) = (&, "i j Aij" ou "j Vj")
	 * Output (Int,Text) = (colonne j, "i Aij" ou "Vj")
	 * Input:Key: Adresse de la ligne du fichier d'input splitté
	 * Input:value: String contenant la ligne du fichier à traiter
	 * context: Sortie du mapper dans laquelle écrire les couples de valeurs (sort)
	 */
	static class FirstMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable column = new IntWritable();
		private Text rowDescription = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] values  = value.toString().split("\\s+"); // "\s" caractère espace

			if (values.length == 3)
			{ // Si on a en entrée un élement de la matrice A: "i j Aij"
				column = new IntWritable(Integer.parseInt(values[1])); // Key: j
				rowDescription = new Text(values[0] + " " + values[2]); //Value: "i Aij"
				//System.out.println("A(" + values[0] + "," + values[1] + ") = " + values[2]);
			}
			else
			{ // Sinon on a un élément du vecteur V: "j Vj"
				column = new IntWritable(Integer.parseInt(values[0])); // Key: j
				rowDescription = new Text(values[1]); //Value: "Vj"
				//System.out.println("V(" + column.toString() + ") = " + rowDescription.toString());
			}
			System.out.println("j = " + column.toString() + " & " + rowDescription.toString());
			context.write(column, rowDescription); // On envoie les valeurs sorted (extraites)
		}
	}

	/*
	 * Input: (Key, Value) = (Int, Iterable<Text>) = (colonne j, "i Aij" ou "Vj")
	 * Output: (Key, Value) = (int, double) = (i, Bij)
	 * Bij = Vj * Aij
	 */
	static class FirstReduce extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

		private DoubleWritable Bij = new DoubleWritable();
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			System.out.println("coucou" + key.toString());
			// On cherche le Vj et stocke les (i,Aij)
			double Vj = 0.;
			HashMap<Integer, Double> A = new HashMap<Integer, Double>();
			for (Text value : values)
			{
				String[] elements  = value.toString().split("\\s+"); //
				if (elements.length == 1)
				{ // "Vj"
					Vj = Double.parseDouble(elements[0]);
				}
				else
				{ //"i Aij"
					A.put(Integer.parseInt(elements[0]), Double.parseDouble(elements[1]));
				}
			}

			// On génère les Bij = Vj * Aij = Vj * A.get(i) en parcourant toute la colonne j de A

			for( Integer i:A.keySet() )
			{
				  Bij = new DoubleWritable(Vj * A.get(i)); //Bij = Vj * Aij
				  System.out.println("key: " + i + " value: " + Bij.toString());
				  context.write(new IntWritable(i), Bij); // Output (row, Bij)
			}
		}
	}
	/*
	 * On transforme juste les Text en nombres
	 * Input (key, value): (Text, Text) = ("i", "Bij")
	 * Output (Int,Double) = (i, Bij)
	 */
	static class SecondMap extends Mapper<Text, Text, IntWritable, DoubleWritable> {
		private IntWritable row = new IntWritable();
		private DoubleWritable Bij = new DoubleWritable();
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
			row = new IntWritable(Integer.parseInt(key.toString()));
			Bij = new DoubleWritable(Double.parseDouble(value.toString()));
			System.out.println("row i=" + key.toString() + " , Bij = " + value.toString());

			context.write(row, Bij);
		}
	}
	/*
	 * Dans cette classe on va chercher les combinaisons (même travail que reduce)
	 * Input(Int, <Iterable>Double) = (i, <>Bij)
	 * OutPut(Int, Double) = (i, Somme(<>Bij) )
	 */
	static class CombinerForSecondMap extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

			Double BijSum = 0.;
			for(DoubleWritable Bij: values)
			{
				BijSum += Bij.get();
			}
			System.out.println("COMBINER> row i=" + key.toString() + " , BijSum = " + BijSum.toString());
			context.write(key, new DoubleWritable(BijSum));
		}
	}
	/*
	 *
	 * Input(Int, <Iterable>Double) = (i, <>Bij)
	 * OutPut(Int, Double) = (i, SommeDeTousLes(<>Bij) )
	 */
	static class SecondReduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

			Double BijSum = 0.;
			for(DoubleWritable Bij: values)
			{
				BijSum += Bij.get();
			}
			System.out.println("REDUCER2> row i=" + key.toString() + " , BijSum = " + BijSum.toString());
			context.write(key, new DoubleWritable(BijSum));
		}
	}

	public static void job(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		// First job
		Job job1 = Job.getInstance(conf);
		//Types de sortie du mapper (Int,Text) = (colonne j, " i Aij" ou "Vj")
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		//On définit le nom des classes de Mapping et de Reducing pour le premier Job
		job1.setMapperClass(FirstMap.class);
		job1.setReducerClass(FirstReduce.class);

		job1.setInputFormatClass(TextInputFormat.class); //splits a block of data into lines and sends to the map function one line at the time
		job1.setOutputFormatClass(TextOutputFormat.class); //converts the key and value to strings and writes them on one line: string(key) separator string(value)

		FileInputFormat.setInputPaths(job1, new Path[]{new Path(conf.get("initialVectorPath")), new Path(conf.get("inputMatrixPath"))}); //Les chemins vers les matrices en input
		FileOutputFormat.setOutputPath(job1, new Path(conf.get("intermediaryResultPath"))); // la sortie intémédiaire entre les deux jobs (contient la matrice B)

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf);
		//Types de sortie du mapper (Int, Double) =
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);

		job2.setMapperClass(SecondMap.class);
		job2.setReducerClass(SecondReduce.class);

		/* If implementation of the combiner passed the unit test, uncomment the following line*/
		//job2.setCombinerClass(CombinerForSecondMap.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);//splits a block of data into lines. It then performs an additional split of each line (the second split looks for a given separator)
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(conf.get("intermediaryResultPath")));
		FileOutputFormat.setOutputPath(job2, new Path(conf.get("currentVectorPath")));

		job2.waitForCompletion(true);

		FileUtils.deleteQuietly(new File(conf.get("intermediaryResultPath")));
	}

}
