import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class testjdbc {
    public static void main(String[] args) {
        // In-memory DuckDB. Use "jdbc:duckdb:/path/to/db" for persistent storage.
        String url = "jdbc:duckdb:/software/duckdb/testdb";

        try (Connection conn = DriverManager.getConnection(url)) {
            // Disable auto-commit for manual transaction control
            conn.setAutoCommit(false);

            // Create table
            try (PreparedStatement createStmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS users(id INTEGER, name VARCHAR)")) {
                createStmt.execute();
            }

            // Prepare insert statement
            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO users VALUES (?, ?)")) {

                // Example data
                String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve"};

                for (int i = 0; i < names.length; i++) {
                    insertStmt.setInt(1, i + 1);
                    insertStmt.setString(2, names[i]);
                    insertStmt.addBatch(); // Add to batch
                }

                // Execute batch insert
                int[] results = insertStmt.executeBatch();
                System.out.println("Inserted " + results.length + " rows.");
                
                for (int i = 0; i < names.length; i++) {
                    insertStmt.clearParameters();
                    insertStmt.setInt(1, i + 1);
                    insertStmt.setString(2, names[i]);
                    insertStmt.addBatch(); // Add to batch
                }

                // Execute batch insert
                int[] results2 = insertStmt.executeBatch();
                System.out.println("Inserted " + results2.length + " rows.");

                // Commit transaction
                /*
                conn.commit();
                System.out.println("Batch transaction committed successfully.");
                */
                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
