/**
 * Created by Shine N on 19/11/2014.
 */
import java.lang.System;
import java.sql.*;
import java.util.*;
/*
Github repo url: https://github.com/Trung-JCed/Social-Computing.git
 */
public class Recommender {

	@SuppressWarnings("null")
	public static void main( String args[] ) {
		Connection c = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:comp3208.db");
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");

		Recommender test = new Recommender();
		test.meanAdjustedRating(c);
  	}

	public void firstColumn(Connection c){
		Statement s;
		try {
			s = c.createStatement();
			ResultSet rs = s.executeQuery( "SELECT * FROM traindata;" );
			ArrayList <int[]> ratings = new ArrayList<int[]>();
			while ( rs.next() ) {
				int[] rating = new int [3];
				rating[0] = rs.getInt(1);
				rating[1] = rs.getInt(2);
				rating[2] = rs.getInt(3);
				ratings.add(rating);

				System.out.println(rating[0]);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void meanAdjustedRating(Connection connection){
		Statement statement;
		try{
			statement = connection.createStatement();
			int user = 1;
			for(;;) {
				String sql_retrieve = "SELECT rating FROM traindata WHERE userid=" + user;
				ResultSet result = statement.executeQuery(sql_retrieve);

				ArrayList<Integer> rating = new ArrayList<Integer>();
				while (result.next()) {
					rating.add(result.getInt(1));
				}

				double mean = this.calMean(rating);
				double[] adjusted_rating = new double[rating.size()];

				for(int i = 0; i < adjusted_rating.length; i++){
					adjusted_rating[i] = rating.get(i) - mean;
				}

				double temp = 0;
				String sql_update = "INSERT INTO traindata (mean_adjusted_rating) VALUES (" + temp +")";

				for(int j = 0; j < adjusted_rating.length; j++){
					temp = adjusted_rating[j];
					System.out.println(sql_update);
					statement.executeUpdate(sql_update);
				}

				user++;
			}

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


