import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordSizeWordCount{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		private static IntWritable count;
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				String thisH = tokenizer.nextToken();
				count = new IntWritable(thisH.length());
				word.set(thisH);
				context.write(count,word);
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, IntWritable>{
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (Text x: values)
			{
				sum++;
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordSize");

		job.setJar("wordcountwordsize.jar");

		job.setJarByClass(WordSizeWordCount.class);

		job.setMapperClass(Map.class);

		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);

		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		Path output = new Path(args[1]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//The program will then wait for the job to finish, and if the job completes successfully, the method will return true. 
		//If the job encounters any errors or fails during execution, the method will throw an exception, 
		//and it will not return until the job is finished or an error occurs. 
		job.waitForCompletion(true);
	}
}