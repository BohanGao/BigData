import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhaseTwoDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        args = new String[2];
//        args[0] = "/Users/bohangao/Projects/BigData/Hadoop/ReverseIndex/test_data/phase1_out";
//        args[1] = "/Users/bohangao/Projects/BigData/Hadoop/ReverseIndex/test_data/phase2_out";
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(PhaseTwoDriver.class);

        job.setMapperClass(PhaseTwoMapper.class);
        job.setReducerClass(PhaseTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
