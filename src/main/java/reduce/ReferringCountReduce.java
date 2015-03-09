package reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import utils.DateDomainWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ame on 19/02/15.
 */
public class ReferringCountReduce extends MapReduceBase implements Reducer<DateDomainWritable, IntWritable, DateDomainWritable, IntWritable> {
    @Override
    //convert a couple ((date,domain), list of 1) => (date,1)
    public void reduce(DateDomainWritable dateDomainWritable, Iterator<IntWritable> iterator, OutputCollector<DateDomainWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        outputCollector.collect(dateDomainWritable, new IntWritable(1));
    }
}
