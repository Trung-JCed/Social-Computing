import java.lang.System;
import java.sql.*;
import java.util.*;
/*
 * Class for the thread to make the image
 */
class AdjustedRatingThread implements Runnable
{
    private int x, y ;
    protected Connection c;
    protected Thread th;


    /*
     * Passes the values of the thread where it will start making the image
     */
    public AdjustedRatingThread(int x, int y, Connection c)
    {
        this.x = x;
        this.y = y;
        this.c = c;
    }

    /*
     *Thread call this with the start() and actually filling the pixel with the parameters given
     */
    public void run()
    {
        for (int i = x; i <= y; i++)
        {
            System.out.println("Calculating userid " + i );
            this.meanAdjustedRating(c, i);
        }
    }

    public void meanAdjustedRating(Connection connection, int user){
        Statement statement;
        try{
            statement = connection.createStatement();

            String sql_retrieve = "SELECT profileid, rating FROM alldata WHERE userid=" + user;
            ResultSet result = statement.executeQuery(sql_retrieve);

            ArrayList<Integer> rating = new ArrayList<Integer>();
            ArrayList<Integer> productId = new ArrayList<Integer>();
            while (result.next()) {
                rating.add(result.getInt(2));
                productId.add(result.getInt(1));
            }

            double mean = this.calMean(rating);
            for(int j = 0; j < rating.size(); j++){
                double temp = rating.get(j) - mean;
                String sql_update = "UPDATE alldata SET adjrating = "+temp+" WHERE profileid = "+productId.get(j)+" AND userid = "+user;
                // System.out.println(sql_update);
                statement.executeUpdate(sql_update);
            }
            System.out.println("User " + user + " done");


        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    private double calMean(ArrayList<Integer> temp){
        double sum = 0,k = 0;
        double mean;
        for(int i = 0; i < temp.size(); i++){
            sum += temp.get(i);
            k++;
        }
        mean = sum / k;
        return mean;
    }
}