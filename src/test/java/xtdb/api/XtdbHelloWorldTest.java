package xtdb.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import xtdb.test.NodeResolver;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static xtdb.api.XtdbHelloWorld.User;

@ExtendWith(NodeResolver.class)
public class XtdbHelloWorldTest {

    @Test
    public void testInMemory(Xtdb node) throws Exception {
        try (var connection =
                     DriverManager.getConnection(
                             format("jdbc:xtdb://localhost:%d/xtdb", node.getServerPort()),
                             "xtdb", "xtdb"
                     );
             var statement = connection.createStatement()) {

            statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}");

            try (var resultSet = statement.executeQuery("SELECT * FROM users")) {
                var users = new ArrayList<User>();

                while (resultSet.next()) {
                    users.add(new User(resultSet.getString("_id"), resultSet.getString("name")));
                }

                var expectedUsers = List.of(new User("joe", "Joe"), new User("jms", "James"));
                assertEquals(expectedUsers, users);
            }
        }
    }

    @Test
    public void testDataSource(Xtdb node) throws Exception {
        try (var connection = node.getConnection();
             var statement = connection.createStatement()) {

            statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}");

            try (var resultSet = statement.executeQuery("SELECT * FROM users")) {
                var users = new ArrayList<User>();

                while (resultSet.next()) {
                    users.add(new User(resultSet.getString("_id"), resultSet.getString("name")));
                }

                var expectedUsers = List.of(new User("joe", "Joe"), new User("jms", "James"));
                assertEquals(expectedUsers, users);
            }
        }
    }
}
