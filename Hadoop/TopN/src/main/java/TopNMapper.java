import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    TreeMap<FlowBean, Text> treeMap = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
        Text phoneNum = new Text(fields[0]);
        FlowBean flowBean = new FlowBean(Long.parseLong(fields[1]), Long.parseLong(fields[2]));

        treeMap.put(flowBean, phoneNum);
        if(treeMap.size()>10){
            treeMap.remove(treeMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iter = treeMap.keySet().iterator();
        while(iter.hasNext()){
            FlowBean k = iter.next();
            context.write(k, treeMap.get(k));
        }
    }
}
