import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommonFriendsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job1 = Job.getInstance(new Configuration());

        job1.setJarByClass(CommonFriendsDriver.class);

        job1.setMapperClass(PhaseOneMapper.class);
        job1.setReducerClass(PhaseOneReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path("/Users/bohangao/Projects/BigData/Hadoop/CommonFriends/test_data/in"));
        FileOutputFormat.setOutputPath(job1, new Path("/Users/bohangao/Projects/BigData/Hadoop/CommonFriends/test_data/out1"));

        boolean result = job1.waitForCompletion(true);

        if (!result) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(new Configuration());

        job2.setJarByClass(CommonFriendsDriver.class);

        job2.setMapperClass(PhaseTwoMapper.class);
        job2.setReducerClass(PhaseTwoReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("/Users/bohangao/Projects/BigData/Hadoop/CommonFriends/test_data/out1"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/bohangao/Projects/BigData/Hadoop/CommonFriends/test_data/out2"));

        result = job2.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
