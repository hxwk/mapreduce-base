package cn.itcast.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 15:04
 * Desc TODO
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String oid;
    private String pid;
    private Double price;

    @Override
    public String toString() {
        return oid + "\t" + pid + "\t" + price;
    }

    public OrderBean() {
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int i = this.oid.compareTo(o.oid);
        if(i==0){
            int i1 = o.price.compareTo(this.price);
            return i1;
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(oid);
        out.writeUTF(pid);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        oid = in.readUTF();
        pid = in.readUTF();
        price = in.readDouble();
    }
}
