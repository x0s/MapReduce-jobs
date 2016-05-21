package pagerank;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class PublicTests extends BaseTests {
	static Configuration conf;
	// directories for matrix vector multiplicatioon
	static final String inputVector = "data/initialVector";
	static final String inputMatrix = "data/initialMatrix";
	static final String intermediary = "data/intermediaryDirectory";
	static final String output = "data/currentVector";
	static {

		conf = new Configuration();
		conf.set("initialVectorPath", inputVector);
		conf.set("inputMatrixPath", inputMatrix);
		conf.set("intermediaryResultPath", intermediary);
		conf.set("currentVectorPath", output);

	}

	public void testFirstMapper() throws IOException{
		MatrixVectorMult.FirstMap m = new MatrixVectorMult.FirstMap();
		MapDriver<LongWritable, Text, IntWritable, Text> mapDriver = MapDriver.newMapDriver(m);
		mapDriver.withInput(new LongWritable(0),new Text("1 1 0.5"));
		mapDriver.withOutput(new IntWritable(1), new Text("1 0.5"));
		mapDriver.runTest();
	}

	public void testSecondMapper() throws IOException{
		MatrixVectorMult.SecondMap m = new MatrixVectorMult.SecondMap();
		MapDriver<Text, Text, IntWritable, DoubleWritable> mapDriver = MapDriver.newMapDriver(m);
		mapDriver.withInput(new Text("1"),new Text("0.1"));
		mapDriver.withOutput(new IntWritable(1), new DoubleWritable(0.1));
		mapDriver.runTest();
	}

	public void testCombiner() throws IOException {
		MatrixVectorMult.CombinerForSecondMap c = new MatrixVectorMult.CombinerForSecondMap();
		ReduceDriver<IntWritable, DoubleWritable, IntWritable, DoubleWritable> combinerDriver = ReduceDriver.newReduceDriver(c);
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(0.5)) ;
		values.add(new DoubleWritable(0.1));

		combinerDriver.withInput(new IntWritable(1), values);
		combinerDriver.withOutput(new IntWritable(1), new DoubleWritable(0.6));
		combinerDriver.runTest();
	}


	public void testSecondReducer() throws IOException {
		MatrixVectorMult.SecondReduce r = new MatrixVectorMult.SecondReduce();
		ReduceDriver<IntWritable, DoubleWritable, IntWritable, DoubleWritable> reduceDriver = ReduceDriver.newReduceDriver(r);
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(0.25));
		values.add(new DoubleWritable(0.25));
		values.add(new DoubleWritable(0.25));
		values.add(new DoubleWritable(0.25));
		reduceDriver.withInput(new IntWritable(2), values);
		reduceDriver.withOutput(new IntWritable(2), new DoubleWritable(1.0));
		reduceDriver.runTest();
	}


	public void testJobMatrixVectorMult()
	{
		try{
			FileUtils.deleteQuietly(new File(output));
			FileUtils.deleteQuietly(new File(intermediary));
			MatrixVectorMult.job(conf);
			HashMap<Integer, Double> outputVector = readOutputVector(output);
			assertEquals(4, outputVector.size());
			double sum = 0.;
			for(int line:outputVector.keySet())
			{
				sum += outputVector.get(line);
			}
			assertEquals(0.9, sum, 0.001);
		}
		catch (IOException | ClassNotFoundException | InterruptedException e) {
			System.out.println(e.toString());
			fail(e.toString());
		}

	}

}
