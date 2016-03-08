package com.thermatk.vkr;

import java.io.*;

/**
 * Created by Ruslan Boitsov on 08.04.15.
 */

class PeriodPrep {
    private final String cvsSplitBy = ",";
    public static void main(String[] args) {
        PeriodPrep per = new PeriodPrep();
        per.run();
    }

    private void run() {


        String[] followers = "localbtc,bitbox,fbtc,crytr,cbx,icbit,bitkonan,vcx,anxhk,just,kraken,rock,itbit,cotr,hitbtc,bitstamp,bitfinex,mtgox,btce".split(",");

        String csvFile = "/home/thermatk/BTC/Final/Step2/PeriodsFixed.csv";
        BufferedReader br = null;
        String line = "";

        try {
            br = new BufferedReader(new FileReader(csvFile));

            int i = 0;

            File fileStats = new File("/media/thermatk/PROGR/BTCFIN2/stats.txt");
            if (!fileStats.exists()) {
                fileStats.createNewFile();
            }
            FileWriter fwStats = new FileWriter(fileStats.getAbsoluteFile());
            BufferedWriter bwStats = new BufferedWriter(fwStats);

            while ((line = br.readLine()) != null) {
                if(i==0) {

                } else {
                    String[] stringFromCSV = line.split(cvsSplitBy);

                    String leader = stringFromCSV[2].replaceAll("\"", "");
                    long begin = Long.parseLong(stringFromCSV[0]);
                    long end = Long.parseLong(stringFromCSV[1]);

                    String filedir = "/media/thermatk/PROGR/BTCFIN2/" + i;
                    makeFileDir(filedir);

                    writeConfig(filedir,leader,begin,end);

                    BufferedReader brLeader = new BufferedReader(new FileReader("/media/thermatk/PROGR/BTCFIN/Leaders/" + leader + "USD.csv"));
                    int sLeaderCount = writePeriodPrepLeader(brLeader, filedir, begin, end);
                    if (sLeaderCount == 0) {
                        System.out.println("Leader N/A for " + i);
                    }

                    String statsFollowers = "";
                    for(String follower: followers) {
                        if(!follower.equals(leader)) {
                            BufferedReader brFollower = new BufferedReader(new FileReader("/media/thermatk/PROGR/BTCFIN/Followers/." + follower + "USD.csv"));
                            int followerCount = writePeriodPrepFollowers(brFollower, filedir, begin, end, follower);
                            if (followerCount > 0) {
                                statsFollowers +="{"+follower+";"+followerCount+"}";
                            }
                        }
                    }

                    String outString = "Period#: " + i + ", Time: " + (end - begin) + ", Leader: " + sLeaderCount + ", Followers: " + statsFollowers;
                    System.out.println(outString);

                    bwStats.write(outString);
                    bwStats.newLine();
                }
                i++;
            }
            bwStats.close();
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

    private void makeFileDir(String filedir) {
        File dir = new File(filedir);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

    private void writeConfig(String filedir, String leader, long begin, long end) throws IOException {
        File file = new File(filedir + "/perconfig.txt");
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(leader + "," + begin+ "," + end);
        bw.close();
    }

    private int writePeriodPrepFollowers(BufferedReader brFollower, String filedir, long begin, long end, String name) throws IOException {
        int count = 0;
        String lineFollower = "";

        File fFile = new File(filedir + "/" + name + ".csv"); //
        if (!fFile.exists()) {
            fFile.createNewFile();
        }

        FileWriter fw2 = new FileWriter(fFile.getAbsoluteFile());
        BufferedWriter bwFollower = new BufferedWriter(fw2);

        boolean firstline = true;
        while ((lineFollower = brFollower.readLine()) != null) {
            String[] stringFromCSVFollower = lineFollower.split(cvsSplitBy);

            long time = Long.parseLong(stringFromCSVFollower[0]);
            double price = Double.parseDouble(stringFromCSVFollower[1]);
            if (time<begin) {
                continue;
            } else if(time>end) {
                break;
            }
            if(!firstline) {
                bwFollower.newLine();
            } else {
                firstline = false;
            }
            bwFollower.write(time + "," + price);
            count++;
        }
        bwFollower.close();
        return count;
    }

    private int writePeriodPrepLeader(BufferedReader brLeader, String filedir, long begin, long end) throws IOException {
        int sLeaderCount = 0;
        String lineLeader = "";

        File leaderFile = new File(filedir + "/leader.csv"); //
        if (!leaderFile.exists()) {
            leaderFile.createNewFile();
        }

        FileWriter fw2 = new FileWriter(leaderFile.getAbsoluteFile());
        BufferedWriter bwLeader = new BufferedWriter(fw2);
        /////
        boolean firstline = true;
        while ((lineLeader = brLeader.readLine()) != null) {
            String[] stringFromCSVLeader = lineLeader.split(cvsSplitBy);

            long time = Long.parseLong(stringFromCSVLeader[0]);
            double price = Double.parseDouble(stringFromCSVLeader[1]);
            if (time<begin) {
                continue;
            } else if(time>end) {
                break;
            }
            if(!firstline) {
                bwLeader.newLine();
            } else {
                firstline = false;
            }
            bwLeader.write(time + "," + price);
            sLeaderCount++;
        }
        bwLeader.close();
        return sLeaderCount;
    }
}
