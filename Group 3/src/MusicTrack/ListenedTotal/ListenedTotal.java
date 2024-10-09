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

public class ListenedTotal {
  // Define the counters
  public static enum COUNTERS {
    INVALID_RECORD_COUNT
  }

  // Define mapper
  public static class ListenedTotalMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    IntWritable trackId = new IntWritable();
    IntWritable one = new IntWritable(1);
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
      // Set the trackId from record array parts
      trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
      // Check if the input record array has 5 cells (elements) and the track was listened on the radio
      if (parts.length == 5 && Integer.parseInt(parts[LastFMConstants.RADIO]) == 1) {
        context.write(trackId, one); // Write the trackId and 1 to the context
      } else {
        // Add counter for invalid records
        context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
      }
    }
  }

  public static class ListenedTotalReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    // Override the reduce
    public void reduce(IntWritable trackId, Iterable<IntWritable> counts, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
      // Sum the count of track listens
      int totalCount = 0;
      for (IntWritable count : counts) {
        totalCount += count.get();
      }
      // Write the trackId and the total count to the context
      context.write(trackId, new IntWritable(totalCount));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: tracklistenercount <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Number of times the track was listened to in total");
    job.setJarByClass(ListenedTotal.class);
    job.setMapperClass(ListenedTotalMapper.class);
    job.setReducerClass(ListenedTotalReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

