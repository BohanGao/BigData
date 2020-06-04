import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        //find top 2 expensive item for each order_id
        Iterator<NullWritable> iter = values.iterator();
        while(iter.hasNext() && count<2){
            count++;
            context.write(key, iter.next());
            //when iter points to the next item in the same group, 
            //the next k and v will be set into the object key and value;
        }
    }
}
