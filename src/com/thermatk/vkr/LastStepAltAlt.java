package com.thermatk.vkr;

import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.TestUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by thermatk on 20.04.15.
 */
public class LastStepAltAlt {
    String cvsSplitBy = ",";
    double xchangeError = 0.0;

    List<String> finalStats = new ArrayList<String>();

    public static void main(String[] args) {
        LastStepAltAlt step = new LastStepAltAlt();
        step.run();
    }

    public void run() {

        String[] readStats = readFromCSV("/media/thermatk/PROGR/BTCFIN3/stats.txt");

        String[] output = new String[readStats.length];
        for(int i=0;i<readStats.length;i++) {
            output[i]="";
        }


        String[] allFollowers = "localbtc,bitbox,fbtc,crytr,cbx,icbit,bitkonan,vcx,anxhk,just,kraken,rock,itbit,cotr,hitbtc,bitstamp,bitfinex,mtgox,btce".split(",");
        HashMap<String,Integer> mapFollList= makeMap(allFollowers);
        List<String> follList = new ArrayList<String>(allFollowers.length);
        for(int i=0;i<allFollowers.length;i++) {
            follList.add(i,"");
        }

        for(int i=0;i<readStats.length;i++) {
            String[] stringFromConfig = readStats[i].split(cvsSplitBy);
            int periodNum = Integer.parseInt(stringFromConfig[0].split(":")[1].replaceAll(" ", ""));

            String filedir = "/media/thermatk/PROGR/BTCFIN3/" + periodNum;
            String periodData = readConfig(filedir);


            log(periodData,1);


            String leaderName = periodData.split(cvsSplitBy)[0];
            int begin = Integer.parseInt(periodData.split(cvsSplitBy)[1]);
            int end = Integer.parseInt(periodData.split(cvsSplitBy)[2]);

            output[i] = "Period#" + periodNum + ": " + periodData + ", ";



            double[] leaderD = convertToMinutes(readFromCSV(filedir+"/leader.csv"), begin, end);

            log("-c: conv leader success " + leaderD.length,2);

            String[] followers = stringFromConfig[3].split(":")[1].replaceAll(" ","").replaceAll(Pattern.quote("}{"), ",").replaceAll(Pattern.quote("}"), "").replaceAll(Pattern.quote("{"), "").split(",");
            for (int d=0; d< followers.length;d++) {
                followers[d] = followers[d].split(";")[0];
            }

            log("-c: loop followers " + followers.length + " start", 2);

            for(String follower: followers) {

                log("-c: conv "+follower+" start",2);

                double[] followerD = convertToMinutes(readFromCSV(filedir + "/" + follower + ".csv"), begin, end);

                log("-c: conv "+follower+" success" + followerD.length,2);

                double[] mycross = myCCF(leaderD, followerD, 60);
                double corr = mycross[0];
                int lag = (int) mycross[1];
                double error = xchangeError;
                log(corr + cvsSplitBy + lag,1);

                if(Double.isNaN(corr)) {
                    log("Not enough data for "+ follower + " so ignoring",1);
                } else if((Double.compare(corr,0.5) > 0)&&(Double.compare(error,0.99)<0)) {
                    String addOut = "(" +follower+";"+error+";"+corr + cvsSplitBy + lag+ ")";
                    output[i] += addOut;

                    String folOut = periodNum + cvsSplitBy+corr + cvsSplitBy + lag + cvsSplitBy+error+ cvsSplitBy+leaderName;
                    int mapAddr = mapFollList.get(follower);
                    String currMap = follList.get(mapAddr);
                    if(currMap.equals("")) {
                        follList.set(mapAddr,folOut);
                    } else {
                        follList.set(mapAddr,(currMap+";"+folOut));
                    }
                }
            }
        }
        writeToCSV("/media/thermatk/PROGR/BTCFIN3/final.csv", output);
        writeMapListToCSV(mapFollList, follList, "/media/thermatk/PROGR/BTCFIN3");

        String[] finalSt = new String[finalStats.size()];
        finalSt = finalStats.toArray(finalSt);
        writeToCSV("/media/thermatk/PROGR/BTCFIN3/finalStats.csv", finalSt);
    }

    private void log(String log, int level) {
        if(level<3) {
            System.out.println(log);
        }
    }

    private void writeMapListToCSV(HashMap<String,Integer> map, List<String> theList, String filepath) {
        String[] elements = new String[map.size()];
        elements = map.keySet().toArray(elements);
        for(String one: elements) {
            int mapAddr = map.get(one);
            String[] file = theList.get(mapAddr).split(";");
            writeToCSV(filepath+"/fin_"+one+".csv",file);

            String stat = one + cvsSplitBy +makeStat(file,2);
            finalStats.add(stat);
        }
    }

    private String makeStat(String[] csvIn, int index) {
        double[] data = doubleFromStrings(csvIn, index);
        String res = "";
        if(data.length > 1) {
            DescriptiveStatistics stats = new DescriptiveStatistics(data);
            double mean = stats.getMean();
            double stdDev = stats.getStandardDeviation();
            double stdErr = stdDev / Math.sqrt(data.length);
            double pValue = TestUtils.tTest(0.0, data);
            res = mean + cvsSplitBy + stdErr + cvsSplitBy + stdDev + cvsSplitBy + pValue;
        } else {
            res = "not enough data";
        }
        return res;
    }

    private HashMap<String,Integer> makeMap(String[] in) {
        HashMap<String,Integer> out= new HashMap<String,Integer>(in.length);
        for(int i=0;i<in.length;i++) {
            String one = in[i];
            out.put(one,i);
        }
        return out;
    }
    private String readConfig (String filedir) {
        String conf = readFromCSV(filedir + "/perconfig.txt")[0];
        return conf;
    }

    private double[] convertToHours(String[] input, int begin, int end) {
        return convertToPeriod(input, begin, end, 3600);
    }

    private double[] convertToMinutes(String[] input, int begin, int end) {
        return convertToPeriod(input, begin, end, 60);
    }

    private double[] convertToPeriod(String[] input, int begin, int end, int period) {

        log("-d: conv "+input.length+" start",3);


        int minutes = ((end+1) - begin) / period;
        List<Double> resultList = new ArrayList<Double>(minutes);
        List<Integer> numTouched = new ArrayList<Integer>(minutes);
        for (int i=0; i<minutes;i++) {
            resultList.add(i,0.0);
            numTouched.add(i,0);
        }

        log("-d: conv: firstcycle "+minutes+" start",3);

        for(String row : input) {
            int rowTime = Integer.parseInt(row.split(",")[0]);
            double rowPrice = Double.parseDouble(row.split(",")[1]);
            int rel = rowTime - begin;
            int minute = (rel/ period);
            int touched = numTouched.get(minute);
            if(touched == 0) {
                resultList.set(minute, rowPrice);
                numTouched.set(minute, 1);
            } else {
                double prev = resultList.get(minute);
                double sum = prev * touched;
                touched++;
                numTouched.set(minute, touched);
                double newRes = (sum + rowPrice) / touched;
                resultList.set(minute, newRes);
            }
        }

        log("-d: conv: firstcycle success",3);

        log("-d: conv: maincycle start", 3);

        int additions = 0;
        for (int i=0; i<minutes;i++) {

            log("-d: conv: maincycle: in iter " + i,4);


            int val = numTouched.get(i);
            if(val == 0) {
                if(i == 0) {
                    boolean found = false;
                    for(int j=1;((j<minutes)&&(!found));j++) {
                        int nVal = numTouched.get(j);
                        if (nVal>0) {
                            double last = resultList.get(j);
                            for(int t=0;t<j;t++) {
                                resultList.set(t, last);
                                numTouched.set(t,1);
                                additions++;
                            }
                            found = true;
                        }
                    }
                } else {
                    int nVal;
                    boolean fixed = false;
                    double first = resultList.get((i-1));
                    for(int j=i+1;((j<minutes)&&(!fixed));j++) {
                        nVal = numTouched.get(j);
                        if (nVal>0) {
                            int delta = (j - i) + 1;
                            double last = resultList.get(j);
                            double priceSmooth = (last-first) / (double) delta;
                            int c = 1;
                            for(int t=i;t<j;t++) {
                                resultList.set(t, (first + ((double) c) * priceSmooth));
                                numTouched.set(t,1);
                                additions++;
                                c++;
                            }
                            fixed = true;
                        }
                    }
                    if(!fixed) {
                        for(int t=i;t<minutes;t++) {
                            resultList.set(t, first);
                            numTouched.set(t,1);
                            additions++;
                        }
                    }
                }
            }
        }


        log("-d: conv: maincycle "+additions+" success",3);

        xchangeError = ((double) additions) / ((double) minutes);

        Double[] outp = new Double[resultList.size()];
        outp = resultList.toArray(outp);
        return  doubleFromDouble(outp);
    }

    private double averageFromList(List<Double> in) {
        double sum = 0.0;
        for(Double row: in) {
            sum+=row;
        }
        double result = sum / ((double) in.size());
        return result;
    }

    private double[] readFromCSV(String filepath, int index) {
        return doubleFromStrings(readFromCSV(filepath), index);
    }

    private String[] readFromCSV(String filepath) {
        List<String> readFile = new ArrayList<String>();
        try {
            BufferedReader brCSV = new BufferedReader(new FileReader(filepath));
            String nextLine = "";
            while ((nextLine = brCSV.readLine()) != null) {
                readFile.add(nextLine);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] out = new String[readFile.size()];
        out = readFile.toArray(out);
        return  out;
    }

    private double[] doubleFromStrings(String[] data, int index) {
        List<Double> resultList = new ArrayList<Double>();
        if(!((data[0].split(cvsSplitBy).length - 1)<index)) {
            for (String one : data) {
                resultList.add(Double.parseDouble(one.split(cvsSplitBy)[index]));
            }
        } else {
            resultList.add(0.0);
        }

        Double[] outp = new Double[resultList.size()];
        outp = resultList.toArray(outp);
        return doubleFromDouble(outp);
    }

    private void writeToCSV(String filepath, double[] x, double[] y) {
        writeToCSV(filepath,stringFromPairDoubles(x,y));
    }

    private void writeToCSV(String filepath, String in) {
        String[] imit = new String[2];
        imit[0] = in;
        imit[1] = "";
        writeToCSV(filepath,imit);
    }

    private void writeToCSV(String filepath, String[] rows) {
        try {
            File fFile = new File(filepath);
            if (!fFile.exists()) {
                fFile.createNewFile();
            }
            FileWriter fw = new FileWriter(fFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            boolean firstline = true;
            for(String row : rows) {
                if(!firstline) {
                    bw.newLine();
                } else {
                    firstline = false;
                }

                bw.write(row);
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] stringFromPairDoubles(double[] x, double[] y) {
        double[][] intermed = new double[2][];
        intermed[0] = x;
        intermed[1] = y;
        return stringFromDoubles(intermed);
    }

    private String[] stringFromDoubles(double[][] datas) {
        int numRows = datas[0].length;
        List<String> resultList = new ArrayList<String>(datas[0].length);
        for(double one : datas[0]) {
            resultList.add(Double.toString(one));
        }
        int numCols = datas.length;
        if (numCols>1) {
            for(int i = 1;i<numCols;i++) {
                for(int j=0;j<numRows;j++) {
                    String now = resultList.get(j) + "," + Double.toString(datas[i][j]);
                    resultList.set(j, now);
                }
            }
        }
        String[] out = new String[resultList.size()];
        out = resultList.toArray(out);
        return  out;
    }

    private double[] doubleFromDouble(Double[] in) {
        double[] out = new double[in.length];
        int t = 0;
        for(Double d : in) {
            out[t] = d;
            t++;
        }
        return out;
    }

    private double[] myCCF(double[] x, double[] y, int maxDelay) {

        double winval = 0;
        int winsec = 0;
        // positive lag
        for(int delaySeconds=0;delaySeconds<maxDelay; delaySeconds++) {
            int newLength = x.length-delaySeconds;
            double[] newx = new double[newLength];
            double[] newy = new double[newLength];

            for(int i = 0; i<newLength;i++) {
                newx[i]=x[i];
                newy[i]=y[i+delaySeconds];
            }

            double currentCorrCoff= getCorrelation(newx,newy);

            if(delaySeconds == 0) {
                winval = currentCorrCoff;
            } else if(currentCorrCoff > winval) {
                winval = currentCorrCoff;
                winsec = delaySeconds;
            }

        }

        // negative lag
        for(int delaySeconds=0;delaySeconds<maxDelay; delaySeconds++) {
            int newLength = x.length-delaySeconds;
            double[] newx = new double[newLength];
            double[] newy = new double[newLength];

            for(int i = 0; i<newLength;i++) {
                newx[i]=x[i+delaySeconds];
                newy[i]=y[i];
            }

            double currentCorrCoff= getCorrelation(newx,newy);

            if(currentCorrCoff > winval) {
                winval = currentCorrCoff;
                winsec = 0 - delaySeconds;
            }

        }
        double[] result = new double[3];
        result[0] = winval;
        result[1] = winsec;
        log(winval +"," +winsec,3);
        return result;
    }

    private double getCorrelation(double[] x, double[] y) {
        PearsonsCorrelation pCorrObject = new PearsonsCorrelation();
        double corr = pCorrObject.correlation(x,y);
        return corr;
    }
}
