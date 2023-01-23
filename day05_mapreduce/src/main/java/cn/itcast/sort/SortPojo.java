package cn.itcast.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/26 16:16
 * Desc TODO
 */
public class SortPojo implements WritableComparable<SortPojo> {
    private String word;
    private Integer num;

    @Override
    public String toString() {
        return word+"\t"+num;
    }

    public SortPojo() {
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public int compareTo(SortPojo o) {
        /**
         * 排序规则：
         * 如果当前
         * this.word.compareTo(o.word) 升序
         * o.word.compareTo(this.word)  降序
         */
        // 如果当前的第一列相同的情况下，根据第二列升序排列
        // 如果两个对象 compareTo ==0 说明两个值相等。
        int i = o.word.compareTo(this.word);
        if(i==0){
            int i1 = this.num.compareTo(o.num);
            return i1;
        }
        return i;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(num);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        word=in.readUTF();
        num=in.readInt();
    }
}
