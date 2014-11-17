import java.sql.*;
import java.util.ArrayList;

public class Main
{
  @SuppressWarnings("null")
public static void main( String args[] )
  {
    Connection c = null;
    try {
      Class.forName("org.sqlite.JDBC");
      c = DriverManager.getConnection("jdbc:sqlite:comp3208.db");
    } catch ( Exception e ) {
      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      System.exit(0);
    }
    System.out.println("Opened database successfully");
    
    
    Statement s;
	try {
		s = c.createStatement();
		ResultSet rs = s.executeQuery( "SELECT * FROM traindata;" );
		ArrayList <int[]> ratings = new ArrayList<int[]>();
		int[] rating = new int [3];
	    while ( rs.next() ) {
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
}