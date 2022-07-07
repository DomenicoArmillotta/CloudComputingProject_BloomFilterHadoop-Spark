package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.MurmurHash;


public class BloomFilter {
    public static class BloomFilterMapper extends Mapper<Object, Text, Text, IntArrayWritable> {
        private IntWritable[] ff1 ;
        private IntWritable[] ff2 ;
        private IntWritable[] ff3 ;
        private IntWritable[] ff4;
        private IntWritable[] ff5;
        private IntWritable[] ff6 ;
        private IntWritable[] ff7 ;
        private IntWritable[] ff8;
        private IntWritable[] ff9;
        private IntWritable[] ff10 ;

        IntArrayWritable f1;
        IntArrayWritable f2;
        IntArrayWritable f3;
        IntArrayWritable f4;
        IntArrayWritable f5;
        IntArrayWritable f6;
        IntArrayWritable f7;
        IntArrayWritable f8;
        IntArrayWritable f9;
        IntArrayWritable f10;


        /**
         * m's values are taken from first MR job's configuration plus 10 bloom filters
         * already dimensioned were created.
         * Filters are created only once thanks to set-up
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            ff1  = new IntWritable[Integer.parseInt(conf.get("m1.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m1.0"));i++ ){
                ff1[i]= new IntWritable(0);
            }

            ff2  = new IntWritable[Integer.parseInt(conf.get("m2.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m2.0"));i++ ){
                ff2[i]= new IntWritable(0);
            }

            ff3  = new IntWritable[Integer.parseInt(conf.get("m3.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m3.0"));i++ ){
                ff3[i]= new IntWritable(0);
            }

            ff4  = new IntWritable[Integer.parseInt(conf.get("m4.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m4.0"));i++ ){
                ff4[i]= new IntWritable(0);
            }

            ff5  = new IntWritable[Integer.parseInt(conf.get("m5.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m5.0"));i++ ){
                ff5[i]= new IntWritable(0);
            }
            ff6  = new IntWritable[Integer.parseInt(conf.get("m6.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m6.0"));i++ ){
                ff6[i]= new IntWritable(0);
            }

            ff7  = new IntWritable[Integer.parseInt(conf.get("m7.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m7.0"));i++ ){
                ff7[i]= new IntWritable(0);
            }

            ff8  = new IntWritable[Integer.parseInt(conf.get("m8.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m8.0"));i++ ){
                ff8[i]= new IntWritable(0);
            }

            ff9  = new IntWritable[Integer.parseInt(conf.get("m9.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m9.0"));i++ ){
                ff9[i]= new IntWritable(0);
            }

            ff10 = new IntWritable[Integer.parseInt(conf.get("m10.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m10.0"));i++ ){
                ff10[i]= new IntWritable(0);
            }

            //f1 acts as a partial bloom of the given rating for current mapper
            f1 = new IntArrayWritable();
            f2 = new IntArrayWritable();
            f3 = new IntArrayWritable();
            f4 = new IntArrayWritable();
            f5 = new IntArrayWritable();
            f6 = new IntArrayWritable();
            f7 = new IntArrayWritable();
            f8 = new IntArrayWritable();
            f9 = new IntArrayWritable();
            f10 = new IntArrayWritable();
        }

        /**
         * Map method in which rating is passed as a key and it's rounded and a movieid is passed
         * Then filters are created adding elements taking the filters already created in setup
         * with the right m dimension
         * @param key: rating
         * @param value: movieID
         * @param context
         */
        @Override
        public void map(Object key, Text value, Context context) {
            //value is the row of data.tsv
            String record1 = value.toString();
            String[] record = record1.split("\\s+");
            String movieId = record[0];
            //rounding the rating
            String rating  = Double.toString(Math.round(Double.parseDouble(record[1])));
            Configuration conf = context.getConfiguration();
            //pvalue taken from the conf
            double pvalue = Double.parseDouble(conf.get("pvalue"));
            //for each row, we assign the corresponding film to the bloom of the rating
            if(rating.compareTo("1.0")==0){
                //function "addItem" adds the 1s to the selected bloom
                //ff1 acts as a temporary array for the current row --> is not serializable
                addItem(ff1, movieId,Integer.parseInt(conf.get("m"+rating)), pvalue);
            }else if (rating.compareTo("2.0")==0){
                addItem(ff2, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("3.0")==0){
                addItem(ff3, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("4.0")==0){
                addItem(ff4, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("5.0")==0){
                addItem(ff5, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("6.0")==0){
                addItem(ff6, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("7.0")==0){
                addItem(ff7, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("8.0")==0){
                addItem(ff8, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("9.0")==0){
                addItem(ff9, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }else if (rating.compareTo("10.0")==0){
                addItem(ff10, movieId,Integer.parseInt(conf.get("m"+rating)),pvalue);
            }
        }

        /**
         * Clean-up to execute write action only once after the job is finished
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //f1 is a serializable object, so I transform ff1 from (intWritable []) to (arrayIntWritable)
            f1.set(ff1);
            f2.set(ff2);
            f3.set(ff3);
            f4.set(ff4);
            f5.set(ff5);
            f6.set(ff6);
            f7.set(ff7);
            f8.set(ff8);
            f9.set(ff9);
            f10.set(ff10);
            //we write in the context the temporary blooms of the mapper
            context.write(new Text("1.0"),f1);
            context.write(new Text("2.0"),f2);
            context.write(new Text("3.0"),f3);
            context.write(new Text("4.0"),f4);
            context.write(new Text("5.0"),f5);
            context.write(new Text("6.0"),f6);
            context.write(new Text("7.0"),f7);
            context.write(new Text("8.0"),f8);
            context.write(new Text("9.0"),f9);
            context.write(new Text("10.0"),f10);
        }

    }

    /**
     * Used in the MAPPER to add MovieID inside IntWritable filters
     * @param newFilter: filter to populate
     * @param movieId: film's ids
     * @param dimension: m parameter to pass in the hash function
     * @param pvalue: pvalue
     */
    public static void addItem(IntWritable[] newFilter, String movieId, int dimension, double pvalue){
        MurmurHash hasher = new MurmurHash();
        byte[] bytearr = movieId.getBytes();
        //calculation of the number of hash functions
        int k= (int) -(Math.log(pvalue)/Math.log(2));
        k++;
        for (int i = 0; i < k; i++) {
            //calculation of the position where set 1 with different seed
            int position = (hasher.hash(bytearr, 9,  i*50) % (dimension)+dimension)%dimension;
            newFilter[position].set(1);
        }
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        //redefinition of toString
        public String toString() {
            StringBuilder sb = new StringBuilder("");
            for (String s : super.toStrings()) {
                sb.append(s).append(" ");
            }
            return sb.toString();
        }
    }

    public static class BloomFilterReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

        /**
         * In reduce method an empty IntWritable array is created and it's setted to 0. After the final array
         * is populated iterating over values passed from Mapper and a logical or is exectued between filters to
         * obtain the final one
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            //final array used to aggregate result of various mapper
            IntArrayWritable finalArray = new IntArrayWritable();
            IntWritable[] bitArray = new IntWritable[Integer.parseInt(conf.get("m"+key))];
            for (int i=0; i<bitArray.length;i++){
                //set to 0 bitArray with the right m taken from config
                bitArray[i]=new IntWritable(0);
            }

            for(IntArrayWritable tmp : values){
                //do OR between BitArray (all 0's) and all bloom arrays of the corresponding rating (values)
                orFilter(bitArray,tmp);
            }
            finalArray.set(bitArray);
            context.write(key, new Text(finalArray.toString()));
        }
    }

    /**
     * Logical or method that takes two filters and gives the final one with merged values
     * @param finalArray
     * @param array
     */
    public static void orFilter(IntWritable[] finalArray, IntArrayWritable array){
        for( int i =0 ; i<finalArray.length;i++){
            int value = Integer.parseInt(array.get()[i].toString());
            if(value == 1)
                finalArray[i].set(1);
        }

    }
}
