import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String order_id;
    private String product_id;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(String order_id, String product_id, double price) {
        this.order_id = order_id;
        this.product_id = product_id;
        this.price = price;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void set(String order_id, String product_id, double price) {
        this.order_id = order_id;
        this.product_id = product_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result = this.order_id.compareTo(o.getOrder_id());

        if (result == 0) {
            return Double.compare(o.getPrice(), this.price);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(product_id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setOrder_id(dataInput.readUTF());
        this.setProduct_id(dataInput.readUTF());
        this.setPrice(dataInput.readDouble());
    }

    @Override
    public String toString() {
        return order_id + "\t" + product_id + "\t" + price;
    }
}
