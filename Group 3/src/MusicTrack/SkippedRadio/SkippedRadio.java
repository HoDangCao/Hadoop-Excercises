import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SkippedRadio {
  // Define the counters
  public static enum COUNTERS {
    INVALID_RECORD_COUNT
  }
  // Define mapper
  public static class SkippedRadioMapper extends Mapper<Object, Text, Text, IntWritable> {
    Text trackId = new Text();
    IntWritable skippedCount = new IntWritable();
    // Define the class contains id corresponding to columns.
    public class LastFMConstants {
      public static final int USER_ID = 0; // the first element in array
      public static final int TRACK_ID = 1; // the second element in array
      public static final int IS_SHARED = 2;
      public static final int RADIO = 3;
      public static final int IS_SKIPPED = 4;
    }
    
    // Override the mapper class
    public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
    throws IOException, InterruptedException {
      // Split the input with the '|' char
      String[] parts = value.toString().split("[|]");
      // Get the trackId from record array parts
      trackId.set(parts[LastFMConstants.TRACK_ID]);
      // Check if the input record array has enough cells (elements) and the track was skipped on the radio
      if (parts.length == 5 && parts[LastFMConstants.RADIO].equals("1") && parts[LastFMConstants.IS_SKIPPED].equals("1")) {
        skippedCount.set(1);
        context.write(trackId, skippedCount); //write the trackId and skipped count to the context
      } else {
        // add counter for invalid records
        context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
      }
    }
  }

  public static class SkippedRadioReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Override the reduce
    public void reduce(Text trackId, Iterable<IntWritable> skippedCounts,
    Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      int totalSkippedCount = 0;
      for (IntWritable skippedCount : skippedCounts) {
        // Added up number of times the track was skipped
        totalSkippedCount += skippedCount.get();
      }
      // Write the trackId and the total number of times the track was skipped to the context
      context.write(trackId, new IntWritable(totalSkippedCount));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: skippedtracks <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Skipped tracks count");
    job.setJarByClass(SkippedRadio.class);
    job.setMapperClass(SkippedRadioMapper.class);
    job.setReducerClass(SkippedRadioReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


