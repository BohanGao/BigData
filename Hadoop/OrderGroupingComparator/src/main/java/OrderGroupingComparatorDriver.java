import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderGroupingComparatorDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[2];
        args[0] = "/Users/bohangao/Projects/BigData/Hadoop/OrderGroupingComparator/test_data/in";
        args[1] = "/Users/bohangao/Projects/BigData/Hadoop/OrderGroupingComparator/test_data/out";

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OrderGroupingComparatorDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
