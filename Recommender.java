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

        Thread th = new Thread(new AdjustedRatingThread( 1,  100000 ,c), "thread1");
        Thread th2 = new Thread(new AdjustedRatingThread(100001, 200000, c), "thread2");
        //Start making it
        th.start();
        th2.start();

        try
        {
            //Wait for the threads to die
            th.join();
            th2.join();
        }
        catch (InterruptedException e)
        {
            System.out.println("Thread interrupted");
        }
    }
}


