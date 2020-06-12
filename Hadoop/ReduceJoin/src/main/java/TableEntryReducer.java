import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableEntryReducer extends Reducer<Text, TableEntryBean, TableEntryBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableEntryBean> values, Context context) throws IOException, InterruptedException {
        TableEntryBean productBean = new TableEntryBean();
        List<TableEntryBean> orderBeans = new ArrayList<>();
        for (TableEntryBean value : values) {
            if (value.getFlag().equals("Order")) {
                TableEntryBean bean = new TableEntryBean();
                try {
                    BeanUtils.copyProperties(bean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(bean);
            } else {//product
                try {
                    BeanUtils.copyProperties(productBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableEntryBean orderBean : orderBeans) {
            orderBean.setProduct_name(productBean.getProduct_name());
            context.write(orderBean, NullWritable.get());
        }
    }
}
