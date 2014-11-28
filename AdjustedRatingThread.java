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
        try {
            while(rs.next()){
                this.meanAdjustedRating(c);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
}