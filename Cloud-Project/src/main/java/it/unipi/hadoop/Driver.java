package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;

public class Driver {

    public static void main(final String[] args) throws Exception {

        Configuration conf1=new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        conf1.set("pvalue",otherArgs[2]);
        conf1.set("input",otherArgs[0]);

        if (otherArgs.length != 3) {
            System.err.println("Usage: BloomFilter project <input> <output> <pvalue>");
            System.exit(1);
        }

        System.out.println("args[0]: <input>="+otherArgs[0]);
        System.out.println("args[1]: <output>="+otherArgs[1]);
        System.out.println("args[2]: <pvalue>="+otherArgs[2]);

        System.out.println("pvalue : "+otherArgs[2]);

        Job j1=Job.getInstance(conf1);
        j1.setJarByClass(CountingMR.class);
        j1.setMapperClass(CountingMR.NewMapper.class);
        j1.setReducerClass(CountingMR.NewReducer.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        Path outputPath=new Path("/cloudproject/counting");

        FileInputFormat.addInputPath(j1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(j1,outputPath);

        //we wait the end of the first mapreduce
        j1.waitForCompletion(true);

        //JOB2
        //Inserisco in un arraylist i valori in output dal primo reducer
        // rating   n   m   k
        ArrayList<String> listValues = new ArrayList<>();
        listValues= pickFile("hdfs:///cloudproject/counting/part-r-00000");
        //create arrays with all m values of the 10 filters
        int[] takeValues = new int[10];
        for (int i=0; i<listValues.size(); i++){
            String row = listValues.get(i);
            int index = (int)Double.parseDouble(String.valueOf(row.split("\t")[0]))-1;
            takeValues[index] = Integer.parseInt(row.split("\t")[1]);
        }
        //Print values
        for (int i = 0; i < takeValues.length; i++){
            System.out.println("M"+i+" : "+ takeValues[i]);
        }
        Configuration conf2=new Configuration();
        conf2.set("output", otherArgs[1]);
        conf2.set("pvalue", otherArgs[2]);
        Job j2=Job.getInstance(conf2);
        j2.setJarByClass(BloomFilter.class);
        j2.setMapperClass(BloomFilter.BloomFilterMapper.class);
        j2.setReducerClass(BloomFilter.BloomFilterReducer.class);
        j2.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(j2, new Path(otherArgs[0]));
        //N.tot =1,247,686      (8)  311922(4)
        // we partition input in 4 parts
        j2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311922);
        //we pass with config the m value calculated with first mapreduce
        j2.getConfiguration().setInt("m1.0",takeValues[0] );
        j2.getConfiguration().setInt("m2.0",takeValues[1] );
        j2.getConfiguration().setInt("m3.0",takeValues[2] );
        j2.getConfiguration().setInt("m4.0",takeValues[3] );
        j2.getConfiguration().setInt("m5.0",takeValues[4] );
        j2.getConfiguration().setInt("m6.0",takeValues[5] );
        j2.getConfiguration().setInt("m7.0",takeValues[6] );
        j2.getConfiguration().setInt("m8.0",takeValues[7] );
        j2.getConfiguration().setInt("m9.0",takeValues[8] );
        j2.getConfiguration().setInt("m10.0",takeValues[9] );


        //mapper
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BloomFilter.IntArrayWritable.class);
        //reducer
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        Path outputPath1=new Path(otherArgs[1]);
        FileOutputFormat.setOutputPath(j2, outputPath1);
        j2.waitForCompletion(true);
        //we start the test
        TestBloom test = new TestBloom();
        test.testingFilters(j2.getConfiguration(), otherArgs[0], "hdfs:///cloudproject/BloomFilter/part-r-00000", otherArgs[2], takeValues);

    }


    /**
     * Method to read MR first job output file, we use this method for the second job in the mapper
     * @param stringPath
     * @return
     */
    public static ArrayList<String> pickFile(String stringPath){
        ArrayList<String> listValues = new ArrayList<>();
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            InputStream stream = fs.open(new Path(stringPath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            Scanner s = new Scanner(reader);
            while (s.hasNextLine()){
                String line = s.nextLine();
                listValues.add(line);
                reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return listValues;
    }
}
