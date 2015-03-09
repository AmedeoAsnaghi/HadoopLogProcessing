package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Created by ame on 16/02/15.
 *  Video downloads (wmv files) per day in the entire time range. (Aggregate the two video versions - normal and remixed)
 */
public class VideoDownloadedMap  extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
    public final String VIDEO = "/random/video/Star_Wars_Kid.wmv";
    public final String VIDEO_REMIX = "/random/video/Star_Wars_Kid_Remix.wmv";
    private static final String lower = "10/Apr/2003";
    private static final String upper = "26/Nov/2003";

    private final DateFormat dateFormat;

    public VideoDownloadedMap(){
        super();
        dateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    }
    @Override
    public void map(LongWritable key, Text text, OutputCollector<LongWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(text.toString());
        StringTokenizer littleTokenizer;
        String date = "";
        int i;
        String line;
        for(i=0; tokenizer.hasMoreTokens(); i++){
            line = tokenizer.nextToken();
            //take the third element (date and time)
            if(i == 3) {
                //take only the data without the time
                littleTokenizer = new StringTokenizer(line, ":");

                date = littleTokenizer.nextToken().replace("[", "");
            }
            else if(i == 5){
                if(!line.contains("GET")){
                    return;
                }
            }
            else if(i == 6){
                if (line.equals(VIDEO) || line.equals(VIDEO_REMIX)){
                    try{
                        Date lowerDate = dateFormat.parse(lower);
                        Date upperDate = dateFormat.parse(upper);
                        Date dateCheck = dateFormat.parse(date);
                        if((dateCheck.before(upperDate) && dateCheck.after(lowerDate)) ||
                                dateCheck.equals(upperDate) || dateCheck.equals(lowerDate)) {
                            outputCollector.collect(new LongWritable(dateCheck.getTime()), new IntWritable(1));
                        }
                        return;
                    }
                    catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
