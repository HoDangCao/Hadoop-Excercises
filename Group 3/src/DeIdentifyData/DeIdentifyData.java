import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Reducer;

public class DeIdentifyData
{

static Logger  LOGGER = Logger.getLogger(DeIdentifyData.class.getName());
    private static boolean header_check = true;
    public static Integer[] encryptCol={2,3,4,5,6,8};
    private static byte[] key1 = new String("samplekey1234567").getBytes();
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
            //value = PatientID, Name,DOB,Phone Number,Email_Address,SSN,Gender,Disease,weight
            // Check nếu là header thì ghi vào context không tiến hành encrypt và return
            if (header_check) { 
                header_check = !header_check;
                context.write(new Text("Header"), new Text(value.toString()));
                return;
            }
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            List<Integer> list=new ArrayList<Integer>();
            Collections.addAll(list, encryptCol); //  thứ tự những cột mà thực hiện encrypt

            System.out.println("Mapper :: one :"+value);
            String newStr="";
            int counter=1;
            while (itr.hasMoreTokens()) {
                String token=itr.nextToken();
                System.out.println("token"+token);
                System.out.println("i="+counter);
                if(list.contains(counter)){ // kiểm tra nếu counter có trong list thì tiến hành encrypt nếu không thì + chuỗi bình thường
                    if(newStr.length()>0)
                        newStr+=","; 

                    newStr+=encrypt(token, key1);
                 }
                else{
                    if(newStr.length()>0)
                        newStr+=",";

                    newStr+=token;
                }
                counter=counter+1;
            }
            context.write(new Text("Encrypt"), new Text(newStr.toString()));
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // write all each values
            for (Text val : values){
                context.write(key, new Text(val.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
                if (args.length != 2) {
        System.out.println("usage: [input] [output]");
        System.exit(-1);
        }


        Job job = Job.getInstance(new Configuration());
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);
        

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(DeIdentifyData.class);
        job.waitForCompletion(true);
    }
    public static String encrypt(String strToEncrypt, byte[] key)
    {
        try
        {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            
            String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
           
            return encryptedString.trim();
            
        }
        catch (Exception e)
        {
            LOGGER.error("Error while encrypting", e);
        }
        return null;
    }
}