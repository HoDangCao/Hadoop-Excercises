import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ListenedRadio {
	// Define the counters
	public static enum COUNTERS {
		INVALID_RECORD_COUNT,
		TRACK_LISTENED_ON_RADIO_COUNT
	}

	public static class UniqueListenersMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		IntWritable trackId = new IntWritable();
		IntWritable radio = new IntWritable();
		// Define the class contains id corresponding to columns.
		public class LastFMConstants {
			public static final int USER_ID = 0; // the first element in array
			public static final int TRACK_ID = 1; // the second element in array
			public static final int IS_SHARED = 2;
			public static final int RADIO = 3;
			public static final int IS_SKIPPED = 4;
		}

		// Override the mapper
		public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
      		// Split the input with '|' char
			String[] parts = value.toString().split("[|]");
			// Get the trackId and radio from record array parts
			trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
			radio.set(Integer.parseInt(parts[LastFMConstants.RADIO]));
			// Check if the input record has 5 fields, write the trackId and radio to the context
			if (parts.length == 5) {
			context.write(trackId, radio);
			context.getCounter(COUNTERS.TRACK_LISTENED_ON_RADIO_COUNT).increment(1L);
			} else {
			// If not, add counter for invalid records
			context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			}
		}
	}

	public static class UniqueListenersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		// Override the reduce class
		public void reduce(IntWritable trackId, Iterable<IntWritable> radios, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
			// Initialize the count to zero
			int count = 0;
			// Loop through the radio values and add them up
			for (IntWritable radio : radios) {
				count += radio.get();
			}
			// Write the trackId and the count to the context
			IntWritable countWritable = new IntWritable(count);
			context.write(trackId, countWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: numberoflisteners <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Number of times the track was listened to on the radio");
		job.setJarByClass(ListenedRadio.class);
		job.setMapperClass(UniqueListenersMapper.class);
		job.setReducerClass(UniqueListenersReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
		if (success) {
			org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
			System.out.println("No. of Invalid Records: " + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
			System.out.println("No. of times the track was listened to on the radio: " + counters.findCounter(COUNTERS.TRACK_LISTENED_ON_RADIO_COUNT).getValue());
			System.exit(0);
		} else {
			System.exit(1);
		}
	}	
}

