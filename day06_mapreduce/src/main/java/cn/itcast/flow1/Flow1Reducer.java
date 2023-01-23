package cn.itcast.flow1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author itcast
 * Date 2020/12/28 16:16
 * Desc TODO
 */
public class Flow1Reducer extends Reducer<Text,Flow1Bean,Text,Flow1Bean> {
    @Override
    protected void reduce(Text key, Iterable<Flow1Bean> values, Context context) throws IOException, InterruptedException {
         Long upPackCount = 0L;
         Long downPackCount = 0L;
         Long upPayLoadCount = 0l;
         Long downPayLoadCount = 0l;
        Flow1Bean fb = new Flow1Bean();
        for (Flow1Bean flow1Bean : values) {
            upPackCount+=flow1Bean.getUpPackNum();
            downPackCount+=flow1Bean.getDownPackNum();
            upPayLoadCount+=flow1Bean.getUpPayLoad();
            downPayLoadCount+=flow1Bean.getDownPayLoad();
        }
        fb.setUpPackNum(upPackCount);
        fb.setDownPackNum(downPackCount);
        fb.setUpPayLoad(upPayLoadCount);
        fb.setDownPayLoad(downPayLoadCount);
        context.write(key,fb);
    }
}
