package cn.itcast.flow1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 16:06
 * Desc TODO
 */
public class Flow1Bean implements Writable {

    private Long upPackNum = 0l;
    private Long downPackNum = 0l;
    private Long upPayLoad = 0l;
    private Long downPayLoad = 0l;

    @Override
    public String toString() {
        return upPackNum + "\t" +
                downPackNum + "\t" +
                upPayLoad + "\t" +
                downPayLoad + "\t";
    }

    public Flow1Bean() {
    }

    public Long getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(Long upPackNum) {
        this.upPackNum = upPackNum;
    }

    public Long getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(Long downPackNum) {
        this.downPackNum = downPackNum;
    }

    public Long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(Long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public Long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(Long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upPackNum);
        out.writeLong(downPackNum);
        out.writeLong(upPayLoad);
        out.writeLong(downPayLoad);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upPackNum = in.readLong();
        downPackNum = in.readLong();
        upPayLoad = in.readLong();
        downPayLoad = in.readLong();
    }
}
