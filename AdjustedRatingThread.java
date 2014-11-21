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
        HashMap<String, String> column = new HashMap<String, String>();
        column.put("itemid", "INT");
        column.put("mar", "REAL");

        try{
            statement = connection.createStatement();

            this.createTable(statement, "mean_adjusted", column);

            String sql_retrieve = "SELECT profileid, rating FROM traindata WHERE userid=" + user;
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
                String sql_insert = "INSERT INTO MAR (itemid, mar) VALUES ("+productId+", "+temp+")";
                statement.executeUpdate(sql_insert);
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

    private void createTable(Statement stmt, String tableName, HashMap<String, String> column) throws SQLException {
        String sql_create = "CREATE TABLE IF NOT EXISTS comp3208."+tableName;
        stmt.executeUpdate(sql_create);

        Set set = column.entrySet();
        Iterator i = set.iterator();
        while (i.hasNext()){
            Map.Entry map = (Map.Entry) i.next();
            String sql_alter = "ALTER TABLE comp3208."+tableName+" ADD COLUMN "+map.getKey()+" "+map.getValue();
            stmt.executeUpdate(sql_alter);
        }
    }
}