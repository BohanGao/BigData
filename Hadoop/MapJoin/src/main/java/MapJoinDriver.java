import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(TableEntryMapper.class);
        job.setReducerClass(TableEntryReducer.class);

        job.setMapOutputKeyClass(TableEntryBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(TableEntryBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI(args[1]));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
