import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    TreeMap<FlowBean, Text> treeMap = new TreeMap<>();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            treeMap.put(new FlowBean(key.getUpFlow(), key.getDownFlow()), new Text(value));
            if(treeMap.size()>10){
                treeMap.remove(treeMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iter = treeMap.keySet().iterator();
        while(iter.hasNext()){
            FlowBean k = iter.next();
            context.write(treeMap.get(k), k);
        }
    }
}
