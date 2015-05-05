package com.thermatk.vkr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Ruslan Boitsov on 03.04.15.
 */

public class Period {
    public static void main(String[] args) {
        Period per = new Period();
        per.run();
    }

    public void run() {

        String csvFile = "/home/thermatk/BTC/Final/Step2/JavaFunFixedMissing.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new FileReader(csvFile));

            int i = 0;
            String[] titles;

            long previousTime = 0;
            String previousLeader = "";
            String leader ="";
            long leaderTime = 0;
            while ((line = br.readLine()) != null) {
                if(i==0) {
                    titles = line.split(cvsSplitBy);
                    System.out.println("\""+titles[0] + "\",\"" + titles[21] + "\"");
                } else {
                    String[] stringFromCSV = line.split(cvsSplitBy);
                    //
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    //Cut off the hours, minutes and seconds
                    Date date = dateFormat.parse(stringFromCSV[0].split(" ")[0]);
                    leaderTime = date.getTime() / 1000;
                    leader = stringFromCSV[21];

                    if (previousLeader.equals(leader)) {
                        //do nothing, continue
                    } else if(previousLeader.equals("")) {
                        previousLeader = leader;
                        previousTime = leaderTime;
                    } else {
                        System.out.println(previousTime +","+(leaderTime - 1) +",\"" + previousLeader + "\"");
                        previousLeader = leader;
                        previousTime = leaderTime;
                    }
                }
                i++;
            }

            if (previousLeader.equals(leader)) {
                System.out.println(previousTime +","+leaderTime +",\"" + previousLeader + "\"");
            } else {
                System.out.println(previousTime +","+(leaderTime - 1) +",\"" + previousLeader + "\"");
                System.out.println(leaderTime +","+(leaderTime + 24*60*60) +",\"" + leader + "\"");
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
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
}
