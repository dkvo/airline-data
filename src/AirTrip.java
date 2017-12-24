import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Created by dang khoa vo on 3/13/17.
 */
public class AirTrip {

    public static void main (String[] args) {
        Connection conn;
        Connection conn1;
        Scanner in = new Scanner(System.in);
        int input;

        conn = DBConnection.getConnection();
        conn1 = DBConnection.getConnection1();
        // menu to select option to run
            do {
                System.out.println("Choose any Option from menu: ");
                System.out.println("Create Database(Press 1)");
                System.out.println("Create and fill AirPlaneTable(Press 2)");
                System.out.println("Create and fill PassengerTable(Press 3)");
                System.out.println("Create and fill FlightTable(Press 4)");
                System.out.println("Insert into AirPlaneTable(Press 5)");
                System.out.println("Insert into PassengerTable(Press 6)");
                System.out.println("Insert into FlightTable(Press 7)");
                System.out.println("create View and insert from view into AirPlaneTable(Press 8)");
                System.out.println("Update AirPlaneTable(Press 9)");
                System.out.println("Update PassengerTable(Press 10)");
                System.out.println("Update FlightTable(Press 11)");
                System.out.println("Delete from  AirPlaneTable(Press 12)");
                System.out.println("Delete from PassengerTable(Press 13)");
                System.out.println("Delete from FlightTable(Press 14)");
                System.out.println("Join Flight and AirPlane tables(Press 15)");
                System.out.println("Join Flight and AirPlane  and Passenger tables(Press 16)");
                System.out.println("Select data by Airplane id(Press 17)");
                System.out.println("Select data by passenger id(Press 18)");
                System.out.println("Select flight by airplane and passenger id(Press 19)");
                System.out.println("Count airplane with larger capacity than 200(Press 20)");
                System.out.println("Create a trigger after delete from Flight table(Press 21)");
                System.out.println("Exit the program(Press 22)");
                // run method upon entering input
                switch (input = in.nextInt()) {
                    case 1:
                        createDatabase(conn);
                        break;
                    case 2:
                        createAirPlane(conn1);
                        break;
                    case 3:
                        createPassenger(conn1);
                        break;
                    case 4:
                        createFlight(conn1);
                        break;
                    case 5:
                        insertAirPlane(conn1);
                        break;
                    case 6:
                        insertPassenger(conn1);
                        break;
                    case 7:
                        insertFlight(conn1);
                        break;
                    case 8:
                        createView(conn1);
                        break;
                    case 9:
                        updateAirPlane(conn1);
                        break;
                    case 10:
                        updatePassenger(conn1);
                        break;
                    case 11:
                        updateFlight(conn1);
                        break;
                    case 12:
                        deleteAirPlane(conn1);
                        break;
                    case 13:
                        deletePassenger(conn1);
                        break;
                    case 14:
                        deleteFlight(conn1);
                        break;
                    case 15:
                        twoWayJoin(conn1);
                        break;
                    case 16:
                        threeWayJoin(conn1);
                        break;
                    case 17:
                        selectAirplane(conn1);
                        break;
                    case 18:
                        selectPassenger(conn1);
                        break;
                    case 19:
                        selectFlight(conn1);
                        break;
                    case 20:
                        countAirplane(conn1);
                        break;
                    case 21:
                        createTrigger(conn1);
                        break;
                    case 22:
                        System.exit(0);
                }

            }while (input < 17);
            try {
                if(conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

    }

    private static void createTrigger(Connection conn) {

        /* sql statment to create trigger that delete row from passenger table whenever a row from
           flight table is delete
        */
        String sql = "CREATE trigger cleanup \n" +
                     "after delete on Flight  FOR EACH ROW\n" +
                     "begin\n" +
                     "delete from Passenger where passenger_id = old.passenger_id;" +
                "\n" +
                     "end;";
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);

            if (stmt != null)
                stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void countAirplane(Connection conn) {
        // sql statement to count the toal of airplane with capacity larger than 200
        String sql = "SELECT COUNT(airplane_id) as total FROM AirPlane WHERE capacity > 200";
        int count = 0;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            // get result from the executed statement
            while (rs.next()) {
                count = rs.getInt("total");
            }
            System.out.println("the total number of airplane with capacity larger than 200 are: " + count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void selectFlight(Connection conn) {
        // sql statement to select everything from flight table  where user enter airplane id and passenger id
        String sql = "SELECT * FROM Flight WHERE airplane_id = ? AND passenger_id = ?";
        String arrival_city;
        String departure_city;
        String arrival_time;
        String departure_time;
        // get input from user to use as airplane id and passenger id
        Scanner in = new Scanner(System.in);
        System.out.print("Insert airplane id: ");
        String airplane_id = in.next();
        in.nextLine();
        System.out.print("Insert passenger id: ");
        String passenger_id = in.next();
        // user prepared statement to execute query with user inputs
        try {
            PreparedStatement preparedstmt = conn.prepareStatement(sql);
            preparedstmt.setString(1, airplane_id);
            preparedstmt.setString(2, passenger_id);
            ResultSet rs = preparedstmt.executeQuery();
            // get result from the executed statement
            while (rs.next()) {
                arrival_city = rs.getString("arrival_city");
                departure_city = rs.getString("departure_city");
                arrival_time = rs.getString("arrival_time");
                departure_time = rs.getString("departure_time");

                System.out.println("airplane id is: " + airplane_id + " | passenger id is: " + passenger_id +
                        " | arrival city is: " + arrival_city + " | departure city is: " + departure_city
                        + " | arrival time is:" + arrival_time + " | departure time is:" + departure_time);
            }
            if (preparedstmt != null)
                preparedstmt.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void selectPassenger(Connection conn) {
        // query to select all from passenger table with user input id
        String sql = "SELECT * FROM Passenger WHERE passenger_id = ?";
        // get id from input
        Scanner in = new Scanner(System.in);
        System.out.print("Insert passenger id: ");
        String passenger_id = in.next();
        String name;
        String email;
        String phone_number;
        // execute query using prepared statement
        try {
            PreparedStatement preparedstmt = conn.prepareStatement(sql);
            preparedstmt.setString(1, passenger_id);
            ResultSet rs = preparedstmt.executeQuery();
            // get result and print result
            while (rs.next()) {
                name = rs.getString("name");
                email = rs.getString("email");
                phone_number = rs.getString("phone_number");

                System.out.println("passenger id is: " + passenger_id + " | name is: " + name +
                        " | email is: " + email + " | phone number is: " + phone_number);
            }
            if (preparedstmt != null)
                preparedstmt.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void selectAirplane(Connection conn) {
        // query to select all from airplane table with user input id
        String sql = "SELECT * FROM AirPlane WHERE airplane_id = ?";
        // get user input
        Scanner in = new Scanner(System.in);
        System.out.print("Insert airplane id: ");
        String airplane_id = in.next();
        String airline_name;
        String airplane_name;
        int capacity;
        // execute query using prepared statement
        try {
            PreparedStatement preparedstmt = conn.prepareStatement(sql);
            preparedstmt.setString(1, airplane_id);
            ResultSet rs = preparedstmt.executeQuery();
            // get result and print output
            while (rs.next()) {
                airline_name = rs.getString("airline_name");
                airplane_name = rs.getString("airplane_name");
                capacity = rs.getInt("capacity");

                System.out.println("airplane id is: " + airplane_id + " | airline name is: " + airline_name +
                                   " | air plane name is: " + airplane_name + " | capacity is: " + capacity);
            }
            if (preparedstmt != null)
                    preparedstmt.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void threeWayJoin(Connection conn) {

        /* query to do inner join of 3 tables, join flight and airplane table on airplane id and passenger with
          flight table on passenger id */

        String sql = "Select airline_name, airplane_name, capacity, Passenger.passenger_id, " +
                "arrival_time, departure_time, arrival_city, departure_city, name, email, phone_number from Airplane " +
                "inner join Flight on Airplane.Airplane_id = Flight.Airplane_id " +
                "inner join Passenger on + Flight.passenger_id = Passenger.passenger_id";
        Statement stmt;
        try { // execute the query
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("airline_name\t | \t airplane_namet\t | \t capacity    |\t passenger_id    |\t" +
                    " arrival_time\t | \t departure_time\t | \t arrival_city\t | \t departure_city\t | \t name\t | \t email\t | \t phone_number");
            while (rs.next()) { // retrieve result and display them
                String airline_name = rs.getString("airline_name");
                String airplane_name = rs.getString("airplane_name");
                int capacity = rs.getInt("capacity");
                String passenger_id = rs.getString("passenger_id");
                String arrival_time = rs.getString("arrival_time");
                String departure_time = rs.getString("departure_time");
                String arrival_city = rs.getString("arrival_city");
                String departure_city = rs.getString("departure_city");
                String name = rs.getString("name");
                String email = rs.getString("email");
                String phone_number = rs.getString("phone_number");
                System.out.println(airline_name + "\t\t" + airplane_name + "\t\t\t" + capacity + "\t\t"
                        + passenger_id + "\t\t" + arrival_time + "\t\t" +  departure_time + "\t\t" + arrival_city + "\t\t" + departure_city
                        + "\t\t" + name + "\t\t" + email + "\t\t" + phone_number);
            }
            if (stmt != null)
                stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void twoWayJoin(Connection conn) {
        // query to join 2 table flight and airplane using airplane id
        String sql = "Select airline_name, airplane_name, capacity, passenger_id, arrival_time, departure_time, arrival_city," +
                     " departure_city from Airplane inner join Flight on Airplane.Airplane_id = Flight.Airplane_id;";
        Statement stmt;
        try { // execute query
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("airline_name\t | \t airplane_namet\t | \t capacity    |\t passenger_id    |\t" +
                    " arrival_time\t | \t departure_time\t | \t arrival_city\t | \t departure_city");
            while (rs.next()) { // get result and display
                String airline_name = rs.getString("airline_name");
                String airplane_name = rs.getString("airplane_name");
                int capacity = rs.getInt("capacity");
                String passenger_id = rs.getString("passenger_id");
                String arrival_time = rs.getString("arrival_time");
                String departure_time = rs.getString("departure_time");
                String arrival_city = rs.getString("arrival_city");
                String departure_city = rs.getString("departure_city");
                System.out.println(airline_name + "\t\t" + airplane_name + "\t\t\t" + capacity + "\t\t"
                + passenger_id + "\t\t" + arrival_time + "\t\t" +  departure_time + "\t\t" + arrival_city + "\t\t" + departure_city);
            }
            if (stmt != null)
                stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void deleteFlight(Connection conn) {
        // query to delete a row from flight with user input airplane id and passenger id
        String sql = "DELETE FROM AirPlane Where airplane_id = ? and passenger_id = ?";
        Scanner in = new Scanner(System.in);
        // get inputs
        System.out.print("Insert airplane id: ");
        String airplane_id = in.next();
        System.out.print("Insert passenger id: ");
        String passenger_id = in.next();

        try { // execute statement
            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            preparedStmt.setString(1, airplane_id);
            preparedStmt.setString(2, passenger_id);
            preparedStmt.execute();
            if (preparedStmt != null)
                preparedStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void deletePassenger(Connection conn) {
        // query to delete a row from passenger table with user input id
        String sql = "DELETE FROM Passenger Where passenger_id = ?";
        Scanner in = new Scanner(System.in);
        System.out.print("Insert passenger id: ");
        String passenger_id = in.next();

        try { // execute query
            try (PreparedStatement preparedStmt = conn.prepareStatement(sql)) {
                preparedStmt.setString(1, passenger_id);
                preparedStmt.execute();
                if (preparedStmt != null)
                    preparedStmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void deleteAirPlane(Connection conn) {
        // query to delete a row from airplane table with user input id
        String sql = "DELETE FROM AirPlane Where airplane_id = ?";
        Scanner in = new Scanner(System.in);
        System.out.print("Insert airplane id: ");
        String airplane_id = in.next();

        try { // execute query
            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            preparedStmt.setString(1, airplane_id);
            preparedStmt.execute();
            if (preparedStmt != null)
                preparedStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void updateFlight(Connection conn) {
        Scanner in = new Scanner(System.in);
        String airplane_id;
        String passenger_id;
        String arrival_city;
        String departure_city;
        String arrival_time;
        String departure_time;

        // get user inputs
        System.out.print("Insert airplane id: ");
        airplane_id = in.next();
        System.out.print("Insert passenger id: ");
        passenger_id = in.next();
        System.out.print("Insert arrival city: ");
        arrival_city = in.next();
        System.out.println("Insert departure city:");
        departure_city = in.next();
        in.nextLine();
        System.out.print("Insert arrival time: ");
        arrival_time = in.nextLine();
        System.out.print("Insert departure time:");
        departure_time = in.nextLine();
        // query to update a row in passenger table with user inputs
        String sql = "UPDATE Flight SET arrival_city = ?, departure_city = ?, arrival_time = ?, departure_time = ? " +
                     "WHERE airplane_id = ? and passenger_id = ?";
        try { // execute query
            try (PreparedStatement preparedStmt = conn.prepareStatement(sql)) {
                preparedStmt.setString(1, arrival_city);
                preparedStmt.setString(2, departure_city);
                preparedStmt.setString(3, arrival_time);
                preparedStmt.setString(4, departure_time);
                preparedStmt.setString(5, airplane_id);
                preparedStmt.setString(6, passenger_id);
                preparedStmt.execute();
                if (preparedStmt != null)
                    preparedStmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void updatePassenger(Connection conn) {
        Scanner in = new Scanner(System.in);
        String passenger_id;
        String name;
        String email;
        String phone_number;
        // get user inputs
        System.out.print("Insert passenger id: ");
        passenger_id = in.next();
        System.out.print("Insert passenger name: ");
        name = in.next();
        System.out.print("Insert passenger email: ");
        email = in.next();
        System.out.println("Insert phone number: ");
        phone_number = in.next();
        // query update a row in passenger table with user inputs
        String sql = "UPDATE Passenger SET name = ?, email = ?, phone_number = ? WHERE passenger_id = ?";
        try { // execute query
            try (PreparedStatement preparedStmt = conn.prepareStatement(sql)) {
                preparedStmt.setString(1, name);
                preparedStmt.setString(2, email);
                preparedStmt.setString(3, phone_number);
                preparedStmt.setString(4, passenger_id);
                preparedStmt.execute();
                if (preparedStmt != null)
                    preparedStmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void updateAirPlane(Connection conn1) {
        // query to update a row from airplane table with users input
        String sql = "UPDATE AirPlane SET airline_name = ?, airplane_name = ?, capacity = ? WHERE airplane_id = ?";

        // get user input
        Scanner in = new Scanner(System.in);
        System.out.print("Insert airplane id: ");
        String airplane_id = in.next();
        System.out.print("Insert airline name: ");
        String airline_name = in.next();
        System.out.print("Insert airplane name: ");
        String airplane_name = in.next();
        System.out.println("Insert capacity: ");
        int capacity = in.nextInt();
        try { // execute input
            PreparedStatement preparedstmt = conn1.prepareStatement(sql);
            preparedstmt.setString(1, airline_name);
            preparedstmt.setString(2, airplane_name);
            preparedstmt.setInt(3, capacity);
            preparedstmt.setString(4, airplane_id);
            preparedstmt.execute();
            if(preparedstmt != null)
                preparedstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createView(Connection conn) {
        // query to create view base on airplane table as the basetable
        String sql = "CREATE VIEW AirPlaneView AS SELECT * FROM AirPlane";
        // query to insert into view, this will change the basetable as well
        String sql1 = "INSERT INTO AirPlaneView(airplane_id, airline_name, airplane_name, capacity) VALUES(?, ?, ?, ?)";
        String airplane_id;
        String airline_name;
        String airplane_name;
        int capacity;
        try { // execute create view query
            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            ResultSet views = conn.getMetaData().getTables(null, null, "AirPlaneView", null);
            if (views.next()) // check if view is already exist
                System.out.println("View already exist");
            else { // execute create view query
                preparedStmt.execute();
            }
            preparedStmt = conn.prepareStatement(sql1);
            System.out.println("do you want to insert value into view?(Y/N)");
            Scanner in = new Scanner(System.in);
            String input = in.next();
            if (input.charAt(0) == 'Y' || input.charAt(0) == 'y') { // insert into view upon request
                // get inputs data
                System.out.print("Insert airplane id: ");
                airplane_id = in.next();
                System.out.print("Insert airline name: ");
                airline_name = in.next();
                System.out.print("Insert airplane name: ");
                airplane_name = in.next();
                System.out.println("Insert capacity: ");
                capacity = in.nextInt();
                // set data to be execute in prepared statement
                preparedStmt.setString(1, airplane_id);
                preparedStmt.setString(2, airline_name);
                preparedStmt.setString(3, airplane_name);
                preparedStmt.setInt(4, capacity);
                preparedStmt.execute(); // execute insert query
                if(preparedStmt != null)
                    preparedStmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private static void insertFlight(Connection conn) {
        Scanner in = new Scanner(System.in);
        String airplane_id;
        String passenger_id;
        String arrival_city;
        String departure_city;
        String arrival_time;
        String departure_time;
        // get user inputs
        System.out.print("Insert airplane id: ");
        airplane_id = in.next();
        System.out.print("Insert passenger id: ");
        passenger_id = in.next();
        System.out.print("Insert arrival city: ");
        arrival_city = in.next();
        System.out.println("Insert departure city:");
        departure_city = in.next();
        in.nextLine();
        System.out.print("Insert arrival time: ");
        arrival_time = in.nextLine();
        System.out.print("Insert departure time:");
        departure_time = in.nextLine();

        // query to insert data into flight table using user inputs
        String sql = "INSERT INTO Flight(airplane_id, passenger_id, arrival_city, departure_city," +
                     " arrival_time, departure_time) VALUES(?, ?, ?, ?, ?, ?)";
        try { // set data to be executed and execute query
            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            preparedStmt.setString(1, airplane_id);
            preparedStmt.setString(2, passenger_id);
            preparedStmt.setString(3, arrival_city);
            preparedStmt.setString(4, departure_city);
            preparedStmt.setString(5, arrival_time);
            preparedStmt.setString(6, departure_time);
            preparedStmt.execute();
            if(preparedStmt != null)
                preparedStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertPassenger(Connection conn) {
        // query to insert data into passenger table using user inputs
        String sql = "INSERT INTO Passenger(passenger_id, name, email, phone_number) VALUES(?, ?, ?, ?)";
        Scanner in = new Scanner(System.in);
        String passenger_id;
        String name;
        String email;
        String phone_number;
        // get user inputs
        System.out.print("Insert passenger id: ");
        passenger_id = in.next();
        System.out.print("Insert passenger name: ");
        name = in.next();
        System.out.print("Insert passenger email: ");
        email = in.next();
        System.out.println("Insert phone number: ");
        phone_number = in.next();

        // set data to be executed and execute query
        try {
            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            preparedStmt.setString(1, passenger_id);
            preparedStmt.setString(2, name);
            preparedStmt.setString(3, email);
            preparedStmt.setString(4, phone_number);
            preparedStmt.execute();
            if(preparedStmt != null)
                preparedStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertAirPlane(Connection conn) {
        Scanner in = new Scanner(System.in);
        String airplane_id;
        String airline_name;
        String airplane_name;
        int capacity;

        // get user inputs
        System.out.print("Insert airplane id: ");
        airplane_id = in.next();
        System.out.print("Insert airline name: ");
        airline_name = in.next();
        System.out.print("Insert airplane name: ");
        airplane_name = in.next();
        System.out.println("Insert capacity: ");
        capacity = in.nextInt();

        // query to insert data into airlane table using user inputs
        String sql = "INSERT INTO AirPlane(airplane_id, airline_name, airplane_name, capacity) VALUES(?, ?, ?, ?)";
        try { // set data to be executed and execute query
            PreparedStatement preparedStmt = conn.prepareStatement(sql);
            preparedStmt.setString(1, airplane_id);
            preparedStmt.setString(2, airline_name);
            preparedStmt.setString(3, airplane_name);
            preparedStmt.setInt(4, capacity);
            preparedStmt.execute();
            if(preparedStmt != null)
                preparedStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createFlight(Connection conn) {
        ResultSet resultSet;
        Statement stmt = null;
        try {
            resultSet = conn.getMetaData().getTables(null, null, "Flight", null);
            if (resultSet.next()) // check if table already exist in database
                System.out.println("Table already exists");
            else { // if it doesn't exist execute query to create the table
                stmt = conn.createStatement();
                String sql = "CREATE TABLE Flight " +
                        "(airplane_id VARCHAR(255), " +
                        "passenger_id VARCHAR(255), " +
                        "arrival_city VARCHAR(255), " +
                        "departure_city VARCHAR(255), " +
                        "arrival_time VARCHAR(255)," +
                        "departure_time VARCHAR(255)," +
                        "FOREIGN KEY (airplane_id) REFERENCES AirPlane(airplane_id)," +
                        "FOREIGN KEY (passenger_id) REFERENCES Passenger(passenger_id))";
                stmt.executeUpdate(sql);
                resultSet.close();
                if (stmt != null)
                    stmt.close();
                System.out.println("do you want to fill table?(Y/N)");
                Scanner in = new Scanner(System.in);
                String input = in.next();
                if (input.charAt(0) == 'Y' || input.charAt(0) == 'y') {
                    fillFlight(conn); // fill data into the table upon user request
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void fillFlight(Connection conn) {
        //
        Statement stmt = null;
        ArrayList<String> list = null;
        Iterator<String> iter = null;
        String query = null;
        // read input file line by line
        try {
            stmt = conn.createStatement();
            System.out.println("Parsing data.");
            list = new CSVReader().readFile("/Users/dangkhoavo/Desktop/CS157B_project2/flight.csv");
            iter = list.iterator();

            while (iter.hasNext()) {
                // get the insert query for each line of the input file
                query = parserFlight(iter.next());
                System.out.println(query);
                if (query != null) {
                    stmt.execute(query); // execute query to fill each line into the table
                }
            }
            if (stmt != null)
                stmt.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String parserFlight(String next) {
        String[] col = next.split(",");
        String airplane_id = col[0];
        String passenger_id = col[1];
        String arrival_city = col[2];
        String departure_city = col[3];
        String arrival_time = col[4];
        String departure_time = col[5];
        // return the insert query with given row from input file
        return "INSERT INTO Flight(airplane_id,passenger_id,arrival_city,departure_city,arrival_time, departure_time) VALUES(" + "'" + airplane_id + "', "
                +"'" + passenger_id + "', " + "'" + arrival_city + "', "  + "'" + departure_city + "', " +
                "'" + arrival_time+ "', " + "'" + departure_time + "'" +")";
    }

    private static void createPassenger(Connection conn) {
        ResultSet resultSet;
        Statement stmt = null;
        try {
            resultSet = conn.getMetaData().getTables(null, null, "Passenger", null);
            if (resultSet.next()) // check if table already exist in database
                System.out.println("Table already exists");
            else { // create table if it doesn't exist
                stmt = conn.createStatement();
                String sql = "CREATE TABLE Passenger " +
                             "(passenger_id VARCHAR(255), " +
                             "name VARCHAR(255), " +
                             "email VARCHAR(255), " +
                             "phone_number VARCHAR(255), " +
                             "PRIMARY KEY (passenger_id))";
                stmt.executeUpdate(sql);
                resultSet.close();
                if (stmt != null)
                    stmt.close();
                System.out.println("do you want to fill table?(Y/N)");
                Scanner in = new Scanner(System.in);
                String input = in.next();
                if (input.charAt(0) == 'Y' || input.charAt(0) == 'y') {
                    fillPassenger(conn); // fill table with data upon user request
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void fillPassenger(Connection conn) {
        Statement stmt = null;
        ArrayList<String> list = null;
        Iterator<String> iter = null;
        String query = null;

        // read input file line by line
        try {
            stmt = conn.createStatement();
            System.out.println("Parsing data.");
            list = new CSVReader().readFile("/Users/dangkhoavo/Desktop/CS157B_project2/passenger.csv");
            iter = list.iterator();

            // get the insert query for each line of input file and execute it
            while (iter.hasNext()) {
                query = parserPassenger(iter.next());
                System.out.println(query);
                if (query != null) {
                    stmt.execute(query);
                }
            }
            if (stmt != null)
                stmt.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static String parserPassenger(String next) {
        String[] col = next.split(",");
        String passenger_id = col[0];
        String name = col[1];
        String email = col[2];
        String phone_number = col[3];
        // return insert query with data from a line of input file
        return "INSERT INTO Passenger(passenger_id,name,email,phone_number) VALUES(" + "'" + passenger_id + "', "
                +"'" + name + "', " + "'" + email + "', "  + "'" + phone_number + "'" + ")";

    }

    private static void createAirPlane(Connection conn) {
        ResultSet resultSet;
        Statement stmt = null;
        try {
            resultSet = conn.getMetaData().getTables(null, null, "AirPlane", null);
            if (resultSet.next()) // check if table already exist
                System.out.println("Table already exists");
            else { // create table if not exist
                stmt = conn.createStatement();
                String sql = "CREATE TABLE AirPlane (airplane_id VARCHAR(255), " +
                        "airline_name VARCHAR(255), " +
                        " airplane_name VARCHAR(255)," +
                        " capacity INTEGER, " +
                        " PRIMARY KEY ( airplane_id ))";
                stmt.executeUpdate(sql);
                resultSet.close();
                if(stmt != null)
                    stmt.close();
                System.out.println("do you want to fill table?(Y/N)");
                Scanner in = new Scanner(System.in);
                String input = in.next();
                if (input.charAt(0) == 'Y' || input.charAt(0) == 'y') {
                    fillAirplane(conn); // fill table with data upon request
                }
            }

        }catch(SQLException e) {
            e.printStackTrace();
        }
    }

    private static void fillAirplane(Connection conn) {
        Statement stmt = null;
        ArrayList<String> list = null;
        Iterator<String> iter = null;
        String query = null;

        try { // read input file line by line into an array list
            stmt = conn.createStatement();
            System.out.println("Parsing data.");
            list = new CSVReader().readFile("/Users/dangkhoavo/Desktop/CS157B_project2/airplane.csv");
            iter = list.iterator();

            while (iter.hasNext()) { // iterate through the list and fill each row of table with data from each line in the list
                query = parserAirPlane(iter.next());
                System.out.println(query);
                if (query != null) {
                    stmt.execute(query);
                }
            }
            if (stmt != null)
                stmt.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String parserAirPlane(String next) {

        String[] col = next.split(",");
        String airplane_id = col[0];
        String airline_name = col[1];
        String airplane_name = col[2];
        int capacity = Integer.parseInt(col[3]);

        // return the insert query with data from a line in the input file
        System.out.println(airplane_id + airline_name + airplane_name + capacity);
        return "INSERT INTO AirPlane(airplane_id,airline_name,airplane_name,capacity) VALUES('" +
                airplane_id + "'," + "'" + airline_name + "'" + "," + "'" + airplane_name + "'" + "," + capacity + ");";
    }

    private static void createDatabase(Connection conn) {

        ResultSet resultSet;
        Statement stmt = null;
        try {
            resultSet = conn.getMetaData().getCatalogs();


             while (resultSet.next()) { // check if data base already exist

                String databaseName = resultSet.getString(1);
                if(databaseName.equals("CS157B")){
                    break;
                }
                else { // if database doesn't exist, create it
                    stmt = conn.createStatement();
                    stmt.executeUpdate("CREATE DATABASE CS157B");
                }
            }
            resultSet.close();
            if(stmt != null)
                stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
