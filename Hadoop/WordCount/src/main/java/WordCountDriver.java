import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.get job instance
        Job job = Job.getInstance(new Configuration());

        //2.set jar location - reflection
        job.setJarByClass(WordCountDriver.class);

        //3.set mapper and reducer class - reflection
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.set output format of map stage - reflection
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.set output format of reduce stage - reflection
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.set MR input and output file path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //optional - use CombineTextInputFormat to combine small size files into one split
        job.setInputFormatClass(CombineFileInputFormat.class);
        //set the max size of virtual split to 4MB
        CombineFileInputFormat.setMaxInputSplitSize(job, 4194304);

        //7.submit job
        boolean result = job.waitForCompletion(true);

        //8.exit with job completion code
        System.exit(result ? 0 : 1);
    }
}
