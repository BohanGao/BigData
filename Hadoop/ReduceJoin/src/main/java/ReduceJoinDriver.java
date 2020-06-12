import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/bohangao/Projects/BigData/Hadoop/ReduceJoin/test_data/in","/Users/bohangao/Projects/BigData/Hadoop/ReduceJoin/test_data/out"};

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ReduceJoinDriver.class);

        job.setMapperClass(TableEntryMapper.class);
        job.setReducerClass(TableEntryReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableEntryBean.class);
        job.setOutputKeyClass(TableEntryBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
