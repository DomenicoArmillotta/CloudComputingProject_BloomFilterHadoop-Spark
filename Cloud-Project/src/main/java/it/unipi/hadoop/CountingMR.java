package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountingMR {

    public static class NewMapper extends Mapper<Object, Text, Text, Text> {
        private final Text reducerKey = new Text();
        private final Text reducerValue = new Text();

        /**
         * Map function that given the input file split each line and save the two tokens (rating, MovieID) inside an array
         * of two elements
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tokens = new String[2];
            //we read the file and split the rating and the title_id
            tokens[0] = record.split("\t")[0];
            tokens[1] = record.split("\t")[1];
            tokens[1] = Double.toString(Math.round(Double.parseDouble(tokens[1])));

            reducerKey.set(tokens[1]);
            reducerValue.set(tokens[0]);
            context.write(reducerKey, reducerValue);
        }
    }



    public static class NewReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * In Reduce method the m is calculated to build filters of the right number of bits, moreover we
         * calulate n by counting films for each rating
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int n = 0,m;
            Configuration conf= context.getConfiguration();
            double pvalue = Double.parseDouble(conf.get("pvalue"));
            //we count the number of film with specific rating
            for (final Text val : values) {
                n++;
            }
            m = (int) (-(float)(n * (float)Math.log(pvalue)) / (float)(Math.pow(Math.log(2), 2))) + 1;
            String mstr = Integer.toString(m);
            String testo =  mstr ;
            Text result = new Text(testo);
            //we create a file with rating and the value "m" , witch is the dimension of bloom
            context.write(key, result);
        }
    }





}

