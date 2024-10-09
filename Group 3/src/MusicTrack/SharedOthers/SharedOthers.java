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

public class SharedOthers {
	// Define for the counters
	public static enum COUNTERS {
		INVALID_RECORD_COUNT
	}

	public static class SharedTracksMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		IntWritable trackId = new IntWritable();
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
			// Get the trackId from record array parts
			trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
      		// Check if the input record array has 5 cells (elements) and the track was shared
			if (parts.length == 5 && parts[LastFMConstants.IS_SHARED].equals("1")) {
				// Combine the trackId and 1 as the key-value pair
				context.write(trackId, new IntWritable(1));
			}
		}
	}

	public static class SharedTracksReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	    // Override the reduce
	    public void reduce(IntWritable trackId, Iterable<IntWritable> shareCounts, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		// Sum the share counts of the each trackIds
		int totalShares = 0;
		for (IntWritable count : shareCounts) {
		    totalShares += count.get();
		}
		// Write the trackId and the total share count to the context
		context.write(trackId, new IntWritable(totalShares));
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
			System.err.println("Usage: sharedtracks <in> <out>");
			System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "Number of times a track was shared");
	    job.setJarByClass(SharedOthers.class);
	    job.setMapperClass(SharedTracksMapper.class);
	    job.setReducerClass(SharedTracksReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}