import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by dangkhoavo on 4/26/17.
 */
public class DBConnection {

    public static Connection getConnection(){

        Connection connection = null;

        // get connection without specifying the database
        try{
            Class.forName("com.mysql.jdbc.Driver");
            try {
                connection = DriverManager.getConnection("jdbc:mysql://localhost/", "root", "Khoavo123");
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }catch (ClassNotFoundException e) {
            System.out.println("Error, driver not found!");
        }

        return connection;
    }

    public static Connection getConnection1(){

        Connection connection = null;

        // get connection while specifying database to be CS157B
        try{
            Class.forName("com.mysql.jdbc.Driver");
            try {
                connection = DriverManager.getConnection("jdbc:mysql://localhost/CS157B", "root", "Khoavo123");
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }catch (ClassNotFoundException e) {
            System.out.println("Error, driver not found!");
        }

        return connection;
    }
}
