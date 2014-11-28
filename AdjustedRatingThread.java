import java.lang.System;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
/*
 * Class for the thread to make the image
 */
class AdjustedRatingThread implements Runnable
{
    private int user;

    protected Connection c;
    private ResultSet rs;

    public AdjustedRatingThread(Connection c, ResultSet rs, boolean turn)
    {
        this.rs = rs;
    //    this.turn = turn;
        user = turn ? 1 : 2;
        this.c = c;
    }

    public void run()
    {
       /* try {
            while(rs.next()){
                this.meanAdjustedRating(c);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } */

          for (int i = 0; i <= 6; i++)
        {
        	System.out.println("Calculating userid " + i );
            this.meanAdjustedRating(c, i);
        }


        // SET SIMILARITY TABLE
    	 for (int i = 1; i <= 5; i++) {
             for (int j = 1; j <= 5; j++) {
                 if (i != j) {
                     this.getSimilarity(i, j, c);
                 }

             }
         }

        this.setPrediction(3,1,c);
        this.setPrediction(2,1,c);
        this.setPrediction(2,2,c);
    }

    public void meanAdjustedRating(Connection connection){
        System.out.println("Calculating userId " + user);

        PreparedStatement s;
        PreparedStatement prestmt;
        try{
            prestmt = connection.prepareStatement("SELECT profileid, rating FROM exercise WHERE userid=?");
            prestmt.setInt(1,user);

            ResultSet result = prestmt.executeQuery();
            HashMap<Integer, Integer> mapA = new HashMap<Integer, Integer>();
            while (result.next()) {
                mapA.put(result.getInt(1), result.getInt(2));
            }
            double mean = this.calMean(mapA);

            Iterator<Entry<Integer, Integer>> it = mapA.entrySet().iterator();

            s = connection.prepareStatement("INSERT INTO exercise_adjusted (userid, itemid, rating, adj) VALUES (?,?,?,?)");

            while (it.hasNext()) {
                Map.Entry<Integer, Integer> pairs = it.next();
                double temp = pairs.getValue() - mean;

                s.setInt(1, user);
                s.setInt(2, pairs.getKey());
                s.setInt(3, pairs.getValue());
                s.setDouble(4, temp);
                s.executeUpdate();
            }
            System.out.println("User " + user + " done");
            user += 2;
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public double calMean(Map<Integer, Integer> mp) {
        double mean;
        double sum = 0;
        int size = mp.size();
        Iterator<Entry<Integer, Integer>> it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Integer> pairs = it.next();
            sum = sum + pairs.getValue();
        }
        mean = sum / size;
        return mean;
    }


}