package reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ame on 16/02/15.
 */
public class SumPerDateReduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    @Override
    public void reduce(LongWritable longWritable, Iterator<IntWritable> iterator, OutputCollector<LongWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while(iterator.hasNext()){
            sum+= iterator.next().get();
        }

        outputCollector.collect(longWritable, new IntWritable(sum));
    }
}
