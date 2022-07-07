package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class TestBloom {

    public static void testingFilters(Configuration conf, String inputFile, String pathBloomFilter, String pval, int[] takeValues) throws IOException {
        System.out.println("\n######### START TESTING... #########\n");
        //read from file system
        FileSystem hadoop = FileSystem.get(conf);
        BufferedReader bloomFilterOutputReader= new BufferedReader(new InputStreamReader(hadoop.open(new Path(pathBloomFilter)))); //to read the filters
        BufferedReader inputFileReader = new BufferedReader(new InputStreamReader(hadoop.open(new Path(inputFile)))); //to read the dataset
        Hash hasher  = new MurmurHash();
        //calculation of number of hash function given pvalue
        int k = (int) (-1*Math.log(Double.parseDouble(pval))/(Math.log(2))) + 1;
        double fp[] = new double[10];
        int numTotFilms = 0;
        int numFilmRating[] = new int[10];
        //creation of bloomFilter with right dimension
        int[][] bloomFilters = createFiltersFromFile(takeValues);

        //creation of bloom reading the output file of hadoop
        try {
            String line;
            line = bloomFilterOutputReader.readLine();
            while (line != null) {
                //key value split with tab
                String[] rowKeyFilter = line.split("\t");
                //round the rating
                String rating = Double.toString(Math.round(Double.parseDouble(rowKeyFilter[0])));
                //having in file n.0, with the regex I take only the first part
                int index = Integer.parseInt(rating.split("\\.")[0]);
                //flter taken from inputFile of selected rating
                String[] filter = rowKeyFilter[1].split(" ");
                //I build blooms by reading from file
                for(int j = 0; j < filter.length; j++) {
                    bloomFilters[index-1][j] = Integer.parseInt(filter[j]);
                }
                line = bloomFilterOutputReader.readLine();
            }
            bloomFilterOutputReader.close();
        }
        catch (IOException e) {
            bloomFilterOutputReader.close();
            throw new RuntimeException(e);
        }
        //compute false positive reading from data.tsv and using blooms created
        try {
            String line;
            line = inputFileReader.readLine();
            while (line != null) {
                numTotFilms++;
                String[] rowIdRating = line.split("\t");
                String filmId = rowIdRating[0];
                //round rating
                int filmRating = (int) Math.round((Double.parseDouble(rowIdRating[1])));
                numFilmRating[filmRating - 1]++;
                Boolean notFoundZero;
                for(int i = 0; i < bloomFilters.length; i++) {
                    notFoundZero = true;
                    for (int j = 0; j < k; j++) {
                        //calculate the position for the k hash function
                        int position = (hasher.hash(filmId.getBytes(), filmId.length(), j) % takeValues[i] + takeValues[i]) % takeValues[i];
                        //check if in all position of all rating(minus the current rating) there is a value different from 1
                        //for each film we check the bloom we know it does not belong to, if it is inside then we increase the FP counter
                        if ((bloomFilters[i][position] != 1) && (i+1 != filmRating)) {
                            notFoundZero = false;
                            //as soon as it finds a 0 it break
                            break;
                        }
                    }
                    //if 0 was not found, then they are all 1s in positions so it is a FP
                    if(notFoundZero && (i+1 != filmRating)){
                        fp[i]++;
                    }
                }
                line = inputFileReader.readLine();
            }
            inputFileReader.close();
        }
        catch (IOException e) {
            inputFileReader.close();
            throw new RuntimeException(e);
        }

        System.out.println("\n######### BLOOMFILTERS' TESTING RESULT #########\n");
        System.out.println("P-VALUE = " + pval+"\n");
        for(int i = 0; i < 10; i++) {
            //compute the false positive rate
            double fpr = fp[i] / (numTotFilms - numFilmRating[i]);
            int j = i + 1;
            System.out.println("Film rating: " + j +"   FPR = " + fpr + " FP = " + fp[i]);
        }
        System.out.println("\n######### TEST ENDED #########\n");




    }

    //creation of bloom filter with right dimension
    private static int[][] createFiltersFromFile(int[] takeValues){
        int[][] bloomFilters = new int[10][];
        for (int i = 0; i < bloomFilters.length; ++i) {
            bloomFilters[i] = new int[takeValues[i]];
        }
        return bloomFilters;
    }
}
