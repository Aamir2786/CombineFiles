import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CombineFiles {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{

        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(new Text(line), new Text(" "));
        }

    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(new Text(line), new Text(" "));
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"Combine Files");
        job.setJarByClass(CombineFiles.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper2.class);
        job.setReducerClass(Reducer1.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

