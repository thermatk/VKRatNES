package com.thermatk.vkr;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by thermatk on 12.04.15.
 */
public class LastStep {
    String cvsSplitBy = ",";
    public static void main(String[] args) throws IOException {
        LastStep step = new LastStep();
        step.run();
    }

    public void run() throws IOException {

        File fFile = new File("/media/thermatk/PROGR/BTCFIN/final.csv"); //
        if (!fFile.exists()) {
            fFile.createNewFile();
        }

        FileWriter fwFinal = new FileWriter(fFile.getAbsoluteFile());
        BufferedWriter bwFinal = new BufferedWriter(fwFinal);

        String configFile = "/media/thermatk/PROGR/BTCFIN/stats.txt";
        BufferedReader br = null;
        String line = "";
        try {
            br = new BufferedReader(new FileReader(configFile));

            while ((line = br.readLine()) != null) {

                String[] stringFromConfig = line.split(cvsSplitBy);
                int periodNum = Integer.parseInt(stringFromConfig[0].split(":")[1].replaceAll(" ", ""));


                ////
                if(periodNum>1) {
                    bwFinal.newLine();
                }
                /////

                String filedir = "/media/thermatk/PROGR/BTCFIN/" + periodNum;
                String periodData = readConfig(filedir);
                System.out.println(periodData);
                long begin = Long.parseLong(periodData.split(cvsSplitBy)[1]);
                long end = Long.parseLong(periodData.split(cvsSplitBy)[2]);

                bwFinal.write("Period#" + periodNum + ": " + periodData + ", ");



                double[] leaderD = fixAndArrayLeader(filedir, begin, end);
                String[] followers = stringFromConfig[3].split(":")[1].replaceAll(" ","").replaceAll(Pattern.quote("}{"), ",").replaceAll(Pattern.quote("}"), "").replaceAll(Pattern.quote("{"), "").split(",");
                for (int d=0; d< followers.length;d++) {

                    followers[d] = followers[d].split(";")[0];
                }
                for(String follower: followers) {
                    double[] followerD = fixAndArrayFollower(filedir,begin,end,follower);
                    PearsonsCorrelation corr = new PearsonsCorrelation();
                    String simplecross = Double.toString(corr.correlation(leaderD, followerD));
                    System.out.println(simplecross);
                    String mycross = myCCF(leaderD,followerD,14400);
                    bwFinal.write("(" +follower+";"+simplecross+";"+mycross+ ")");

                }
            }

            bwFinal.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private String readConfig (String filedir) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filedir + "/perconfig.txt"));
        return br.readLine();
    }


    private double[] fixAndArrayLeader(String filedir, long begin, long end) throws IOException {
        ////////
        BufferedReader brLeader = new BufferedReader(new FileReader(filedir + "/leader.csv"));
        List<Double> finalLeader = new ArrayList<Double>((int)(end-begin));
        long previousTime = 0;
        double previousPrice = 0;
        String leaderLine = "";

        //long repeatedTime = 0;
        //List<Double> repeatedPrices = new ArrayList<Double>();

        while ((leaderLine = brLeader.readLine()) != null) {

            String[] stringFromCSVLeader = leaderLine.split(cvsSplitBy);

            long lineTime = Long.parseLong(stringFromCSVLeader[0]);
            double linePrice = Double.parseDouble(stringFromCSVLeader[1]);
            if(previousTime == 0) {
                int delta = (int)(lineTime - begin);
                for(int j=0;j<delta;j++) {
                    finalLeader.add(linePrice);
                }
            } else if (previousTime == lineTime) {
                //
            } else if ((lineTime - previousTime)>1) {
                int delta = (int)(lineTime - previousTime);
                double priceSmooth = (linePrice-previousPrice) / (double) delta;
                for(int j=1;j<=delta;j++) {
                    finalLeader.add(previousPrice + ((double)j)*priceSmooth);
                }
            } else {
                finalLeader.add(linePrice);
            }

            previousTime = lineTime;
            previousPrice = linePrice;
        }
        if (previousTime < end) {
            int delta = (int)(end - previousTime);
            for(int j=0;j<delta;j++) {
                finalLeader.add(previousPrice);
            }
        }
        Double[] result = new Double[finalLeader.size()];
        result = finalLeader.toArray(result);

        double[] tempArray = new double[result.length];
        int t = 0;
        for(Double d : result) {
            tempArray[t] = d;
            t++;
        }
        return tempArray;
    }

    private double[] fixAndArrayFollower(String filedir, long begin, long end, String name) throws IOException {
        ////////
        BufferedReader brFollower = new BufferedReader(new FileReader(filedir + "/"+name+ ".csv"));
        List<Double> finalFollower = new ArrayList<Double>((int)(end-begin));
        long previousTime = 0;
        double previousPrice = 0;
        String followerLine = "";

        //long repeatedTime = 0;
        //List<Double> repeatedPrices = new ArrayList<Double>();

        while ((followerLine = brFollower.readLine()) != null) {

            String[] stringFromCSVFollower = followerLine.split(cvsSplitBy);

            long lineTime = Long.parseLong(stringFromCSVFollower[0]);
            double linePrice = Double.parseDouble(stringFromCSVFollower[1]);
            if(previousTime == 0) {
                int delta = (int)(lineTime - begin);
                for(int j=0;j<delta;j++) {
                    finalFollower.add(linePrice);
                }
            } else if (previousTime == lineTime) {
                //
            } else if ((lineTime - previousTime)>1) {
                int delta = (int)(lineTime - previousTime);
                double priceSmooth = (linePrice-previousPrice) / (double) delta;
                for(int j=1;j<=delta;j++) {
                    finalFollower.add(previousPrice + ((double)j)*priceSmooth);
                }
            } else {
                finalFollower.add(linePrice);
            }

            previousTime = lineTime;
            previousPrice = linePrice;
        }
        if (previousTime < end) {
            int delta = (int)(end - previousTime);
            for(int j=0;j<delta;j++) {
                finalFollower.add(previousPrice);
            }
        }
        Double[] result = new Double[finalFollower.size()];
        result = finalFollower.toArray(result);

        double[] tempArray = new double[result.length];
        int t = 0;
        for(Double d : result) {
            tempArray[t] = d;
            t++;
        }
        return tempArray;
    }

    private String myCCF(double[] x, double[] y, int maxDelay) {

        double winval = 0;
        int winsec = 0;
        for(int delaySeconds=0;delaySeconds<maxDelay; delaySeconds++) {
            int newLength = x.length-delaySeconds;
            double[] newx = new double[newLength];
            double[] newy = new double[newLength];

            for(int i = 0; i<newLength;i++) {
                newx[i]=x[i];
                newy[i]=y[i+delaySeconds];
            }
            PearsonsCorrelation pCorrObject = new PearsonsCorrelation();
            double currentCorrCoff= pCorrObject.correlation(newx,newy);

            if(delaySeconds == 0) {
                winval = currentCorrCoff;
            } else if(currentCorrCoff > winval) {
                winval = currentCorrCoff;
                winsec = delaySeconds;
            }

        }

        System.out.println(winval +"," +winsec);
        return (winval +"," +winsec);
    }
}
