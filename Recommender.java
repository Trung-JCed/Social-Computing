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

        ResultSet rs = null;
        Statement smt;
        try {
            smt = c.createStatement();
            rs = smt.executeQuery("SELECT * FROM traindata;");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Thread odd = new Thread(new AdjustedRatingThread(c, rs, true));
        Thread even = new Thread(new AdjustedRatingThread(c, rs, false));

        odd.start();
        even.start();
    }
}


