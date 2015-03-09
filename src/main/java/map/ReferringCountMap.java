package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import utils.DateDomainWritable;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Created by ame on 19/02/15.
 *  Number of referring domains per day (between the 22nd of April and the 30th of May)
 */
public class ReferringCountMap extends MapReduceBase implements Mapper<LongWritable, Text, DateDomainWritable, IntWritable> {
    private final DateFormat dateFormat;
    private static final String lower = "22/Apr/2003";
    private static final String upper = "30/May/2003";
    public ReferringCountMap(){
        super();
        dateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    }

    @Override
    public void map(LongWritable key, Text text, OutputCollector<DateDomainWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(text.toString());
        StringTokenizer littleTokenizer;
        String date = "";
        String token;

        for(int i=0; tokenizer.hasMoreTokens(); i++){
            token = tokenizer.nextToken();
            //take the third element (date and time)
            if(i == 3) {
                //take only the data without the time
                littleTokenizer = new StringTokenizer(token, ":");

                date = littleTokenizer.nextToken().replace("[", "");
            }
            else if(i == 5){
                if(!token.contains("GET")){
                    return;
                }
            }
            else if(i == 10){
                //we consider the cases only with a specific domain
                if(token.equals("\"-\""))
                    return;

                String domain="";
                String finalDomain ="";
                boolean withHttp = false;
                littleTokenizer = new StringTokenizer(token,"/");

                for(int j=0; littleTokenizer.hasMoreTokens(); j++){
                    domain = littleTokenizer.nextToken();
                    if(j==0){
                        if(domain.equals("\"http:") || domain.equals("\"https:")){
                            //if the string contain http or https, i need to check the domain on the next token
                            withHttp = true;
                        }
                        else {
                            StringTokenizer dotTokenizer = new StringTokenizer(domain,".");
                            String lineDot;
                            boolean domainOk = false;
                            for(int k=0; dotTokenizer.hasMoreTokens();k++){
                                lineDot = dotTokenizer.nextToken();
                                // ********CASE www.*
                                if (k==0 && lineDot.length()>0 && !lineDot.equals("www")){
                                    finalDomain = lineDot;
                                }
                                //*********CASE *.aa || *.aaa
                                else if (k>0){
                                    if(!finalDomain.equals("")){
                                        finalDomain = finalDomain + "." + lineDot;
                                    }
                                    else{
                                        finalDomain = lineDot;
                                    }
                                    domainOk = true;
                                }
                            }

                            if(domainOk){
                                try{
                                    Date lowerDate = dateFormat.parse(lower);
                                    Date upperDate = dateFormat.parse(upper);
                                    Date dateCheck = dateFormat.parse(date);
                                    if((dateCheck.before(upperDate) && dateCheck.after(lowerDate)) ||
                                            dateCheck.equals(upperDate) || dateCheck.equals(lowerDate)) {

                                        outputCollector.collect(new DateDomainWritable(dateCheck.getTime(),finalDomain),
                                                new IntWritable(1));
                                    }
                                    return;
                                }
                                catch (ParseException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                    }
                    else if(j==1){

                        if(withHttp){
                            StringTokenizer dotTokenizer = new StringTokenizer(domain,".");
                            String lineDot;
                            boolean domainOk = false;
                            for(int k=0; dotTokenizer.hasMoreTokens();k++){
                                lineDot = dotTokenizer.nextToken();
                                // ********CASE www.*
                                if (k==0 && lineDot.length()>0 && !lineDot.equals("www")){
                                    finalDomain = lineDot;
                                }
                                //*********CASE *.aa || *.aaa
                                else if (k>0){
                                    if(!finalDomain.equals("")){
                                        finalDomain = finalDomain + "." + lineDot;
                                    }
                                    else{
                                        finalDomain = lineDot;
                                    }
                                    domainOk = true;
                                }
                            }

                            if(domainOk){
                                try{
                                    Date lowerDate = dateFormat.parse(lower);
                                    Date upperDate = dateFormat.parse(upper);
                                    Date dateCheck = dateFormat.parse(date);
                                    if((dateCheck.before(upperDate) && dateCheck.after(lowerDate)) ||
                                            dateCheck.equals(upperDate) || dateCheck.equals(lowerDate)) {

                                        outputCollector.collect(new DateDomainWritable(dateCheck.getTime(),finalDomain),
                                                new IntWritable(1));
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
        }
    }
}
