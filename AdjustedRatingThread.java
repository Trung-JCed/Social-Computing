import java.lang.System;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

class AdjustedRatingThread implements Runnable
{
    private int x, y ;
    protected Connection c;
    protected Thread th;



    public AdjustedRatingThread(int x, int y, Connection c)
    {
        this.x = x;
        this.y = y;
        this.c = c;
    }


    public void run()
    {
       /* for (int i = 0; i <= 6; i++)
        {
        	System.out.println("Calculating userid " + i );
            this.meanAdjustedRating(c, i);
        } 
    	*/

        // SET SIMILARITY TABLE
    	/* for (int i = 1; i <= 5; i++)
         {
    		 for (int j = 1; j <= 5; j++)
    		 {
    			 if (i != j) {
    	    			this.getSimilarity(i, j, c);
    			 }
    			 
    		 }
         } 
    	 */

        this.setPrediction(3,1,c);
        this.setPrediction(2,1,c);
        this.setPrediction(2,2,c);
    }


    public void getSimilarity(int item1, int item2, Connection c)
    {
        List<Integer> b1 = this.getUsers(item1, c);
        List<Integer> b2 = this.getUsers(item2, c);
        List<Integer> b3 = this.getSameUsers(b1, b2, c);
        ArrayList <Double> a = this.getRatings(b3, item1, c);
        ArrayList <Double> b = this.getRatings(b3, item2, c);
        //System.out.println(this.getSum(a, b) / this.getSumSquare(a, b));

        PreparedStatement s;
        try {
            s = c.prepareStatement("INSERT INTO exercise_similarity"
                    + "(item1, item2, similarity) VALUES"
                    + "(?,?,?)");
            s.setInt(1, item1);
            s.setInt(2, item2);
            s.setDouble(3, this.getSum(a, b) / this.getSumSquare(a, b));
            s.executeUpdate();
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    public void meanAdjustedRating(Connection connection, int user){
        try{
            PreparedStatement prestmt = connection.prepareStatement("SELECT itemid, rating FROM exercise WHERE userid=?");
            prestmt.setInt(1,user);
            //String sql_retrieve = "SELECT profileid, rating FROM alldata WHERE userid=" + user;
            ResultSet result = prestmt.executeQuery();
            HashMap<Integer, Integer> mapA = new HashMap<Integer, Integer>();
            while (result.next()) {
                //System.out.println(result.getInt(1) + " " + result.getInt(2));
                mapA.put(result.getInt(1), result.getInt(2));
            }
            double mean = this.calMean(mapA);
            //System.out.println(mean);

            Iterator<Entry<Integer, Integer>> it = mapA.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Integer> pairs = (Entry<Integer, Integer>)it.next();
                double temp = pairs.getValue() - mean;
                //System.out.println(pairs.getKey() +  " " + pairs.getValue());
                //INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)

                PreparedStatement s = connection.prepareStatement("INSERT INTO exercise_adjusted"
                        + "(userid, itemid, rating, adj) VALUES"
                        + "(?,?,?,?)");
                s.setInt(1, user);
                s.setInt(2, pairs.getKey());
                s.setInt(3, pairs.getValue());
                s.setDouble(4, temp);
                s.executeUpdate();
                // it.remove(); // avoids a ConcurrentModificationException
            }
            System.out.println("User " + user + " done");


            // double mean = this.calMean(rating);



        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public double  calMean(Map<Integer, Integer> mp) {
        double mean;
        double sum = 0;
        int size = mp.size();
        Iterator<Entry<Integer, Integer>> it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Integer> pairs = (Entry<Integer, Integer>)it.next();
            //System.out.println( sum + " + " + pairs.getValue());
            sum = sum + pairs.getValue();
            //it.remove(); // avoids a ConcurrentModificationException
        }
        // System.out.println("mean: " + mean);
        // System.out.println("sum: " + sum);
        //System.out.println("size: " + size);
        mean = sum / size;
        //  System.out.println("mean: " + mean);
        return mean;
    }


    public List<Integer> getUsers(int x, Connection c) {
        List<Integer> l1 = new ArrayList<Integer>();
        try{
            PreparedStatement prestmt = c.prepareStatement("SELECT userid FROM exercise_adjusted WHERE itemid=?");
            prestmt.setInt(1,x);
            ResultSet result = prestmt.executeQuery();
            while (result.next()) {
                l1.add(result.getInt(1));
            }
        }catch (SQLException e){
            e.printStackTrace();
        }

        return l1;
    }

    public List<Integer> getSameUsers(List<Integer> x, List<Integer>y, Connection c) {
        List<Integer> l3 = new ArrayList<Integer>(y);
        l3.retainAll(x);
        return l3;
    }

    public ArrayList<Double> getRatings(List<Integer> list, int x, Connection c) {
        ArrayList <Double> ratings = new ArrayList< Double>();
        Iterator<Integer> iterator = list.iterator();
        while (iterator.hasNext()) {
            int temp = iterator.next();
            try{
                PreparedStatement prestmt = c.prepareStatement("SELECT adj FROM exercise_adjusted WHERE itemid=? and userid=?");
                prestmt.setInt(1,x);
                prestmt.setInt(2, temp);
                ResultSet rs = prestmt.executeQuery();
                while (rs.next()) {
                    ratings.add( rs.getDouble(1));
                }
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        return ratings;
    }

    public double getSum(ArrayList<Double> x, ArrayList<Double> y) {
        double sum = 0;
        for  (int i = 0; i < x.size() ; i++)
        {
            sum = sum + x.get(i) * y.get(i);
        }
        return sum;
    }

    public double getSumSquare(ArrayList<Double> x, ArrayList<Double> y) {
        double sum = 0;
        double l1 = 0;
        double l2 = 0;
        for  (int i = 0; i < x.size() ; i++)
        {
            l1 = l1 + x.get(i) * x.get(i);
            l2 = l2 + y.get(i) * y.get(i);
        }
        sum = Math.sqrt(l1) * Math.sqrt(l2);
        return sum;
    }

    public void setPrediction(Integer item1, Integer user,Connection c) {
        //Rating - key , simlarity - value
        HashMap<Integer, Double> simrating = new HashMap<Integer, Double>();
        PreparedStatement s;
        try {
            s = c.prepareStatement("SELECT similarity, item2 FROM exercise_similarity WHERE item1=?");
            s.setInt(1, item1);
            ResultSet rs = s.executeQuery();
            while (rs.next()) {
                //greater than 0 similarity
                if (rs.getDouble(1) > 0) {
                    //System.out.println(rs.getDouble(1));
                    //System.out.println(rs.getDouble(1) + " " + rs.getInt(2));

                    PreparedStatement stmt = c.prepareStatement("SELECT rating FROM exercise WHERE itemid =? and userid =?");
                    stmt.setInt(1, rs.getInt(2));
                    stmt.setInt(2, user);
                    ResultSet r = stmt.executeQuery();
                    while(r.next()) {
                        simrating.put(r.getInt(1),rs.getDouble(1));
                        //System.out.println(r.getInt(1) + " " + rs.getDouble(1));
                    }
                }
                //ratings.add( rs.getDouble(1));
            }

            System.out.println(this.calculatePrediction(simrating));
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }


    public Double calculatePrediction(Map <Integer, Double> mp) {
        double prediction = 0;
        double x = 0;
        double y = 0;
        Iterator<Entry<Integer, Double>> it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Double> pairs = (Entry<Integer, Double>)it.next();
            x = x + pairs.getKey() * pairs.getValue();
            y = y + pairs.getValue();
        }
        prediction = x / y;
        return prediction;
    }
    
    
    
    
    
    

    /*

    private double calMean(ArrayList<Integer> temp){
        double sum = 0,k = 0;
        double mean;
        for(int i = 0; i < temp.size(); i++){
            sum += temp.get(i);
            k++;
        }
        mean = sum / k;
        return mean;
    }*/
}