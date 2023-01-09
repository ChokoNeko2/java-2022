import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

public class Main {
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        CSVReader reader = new CSVReader(new FileReader("Table.csv"));
        List<String[]> rows = reader.readAll();

        ArrayList<Product> products = new ArrayList<>();
        boolean flag = true;
        for (String[] row : rows) {
            if (flag) {
                flag = false;
                continue;
            }
            Country country = new Country(row[1], row[0]);
            Product p = new Product();
            p.country = country;
            p.Type = row[2];
            p.isOnline = row[3].equals("Online");
            p.OrderPriority = row[4];
            p.OrderDate = row[5];
            p.UnitsSold = Integer.parseInt(row[6]);
            p.TotalProfit = Double.parseDouble(row[7]);
            products.add(p);
        }


//      Class.forName("org.sqlite.JDBC");
        boolean dbExist = Files.exists(Paths.get("demo.db"));

        String jdbcURL = "jdbc:sqlite:demo.db";
        Connection connection = DriverManager.getConnection(jdbcURL);
        Statement statement = connection.createStatement();
        if (!dbExist)
            databaseLogic(products, connection, statement);

        statement.close();


        System.out.println("Задание 2");
        ResultSet rs = statement.executeQuery("Select countryname, max(profit) as m from " + getUnion("Europe", "Asia"));
        System.out.println(rs.getString("countryname") + ", Total Profit = " + rs.getString("m"));
        System.out.println("Задание 3");
        rs = statement.executeQuery("select countryname,profit from " + getUnion("Middle East and North Africa", "Sub-Saharan Africa") + " where profit BETWEEN 420000 AND 440000 ORDER BY profit DESC ");
        System.out.println(rs.getString("countryname") + ", Total Profit = " + rs.getString("profit"));
    }

    private static void databaseLogic(ArrayList<Product> products, Connection connection, Statement statement) throws SQLException {
        statement.execute(
                "CREATE TABLE Products (" +
                        "Region VARCHAR," +
                        "CountryName VARCHAR," +
                        "ItemType VARCHAR," +
                        "SalesChannel BOOLEAN NOT NULL CHECK (SalesChannel IN (0, 1))," +
                        "OrderPriority VARCHAR(1)," +
                        "OrderDate VARCHAR," +
                        "UnitsSold INT," +
                        "Profit REAL);");


        PreparedStatement s = connection.prepareStatement("INSERT INTO Products VALUES (?,?,?,?,?,?,?,?);");

        products.forEach(p -> {
            try {
                s.setString(1, p.country.region);
                s.setString(2, p.country.name);
                s.setString(3, p.Type);
                s.setInt(4, p.isOnline ? 1 : 0);
                s.setString(5, p.OrderPriority);
                s.setString(6, p.OrderDate);
                s.setInt(7, p.UnitsSold);
                s.setDouble(8, p.TotalProfit);
                s.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        s.close();
    }

    public static String getUnion(String t1, String t2) {
        return String.format("(SELECT * from Products where region='%s' UNION SELECT * from Products where region='%s')", t1, t2);
    }

}
