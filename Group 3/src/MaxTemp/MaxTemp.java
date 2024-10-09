import java.io.IOException;
import java.util.StringTokenizer;
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
public class MaxTemp {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text year_col= new Text();
        private IntWritable temp_col= new IntWritable();
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
            String values[]=value.toString().split(" ");  // tách dữ liệu 
            year_col.set(values[0]);
            temp_col.set(Integer.parseInt(values[1]));
            context.write(year_col,temp_col);
        }
}

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
   
    public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
        int maxtemp=0;
        for(IntWritable val : values) {
            int temperature= val.get();
            if(maxtemp<temperature) //  so sánh và tìm maxtemp 
            {
                 maxtemp =temperature;
            }
        }
        context.write(key, new IntWritable(maxtemp));
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "MaxTemp");
    job.setJarByClass(MaxTemp.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    Path outputPath = new Path(args[1]);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    outputPath.getFileSystem(conf).delete(outputPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}