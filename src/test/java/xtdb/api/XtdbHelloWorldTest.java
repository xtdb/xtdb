package xtdb.api;

import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static xtdb.api.XtdbHelloWorld.*;

public class XtdbHelloWorldTest {

    @Test
    public void testInMemory() throws Exception {
        var serverConfig = new ServerConfig();
        serverConfig.setPort(0);
        var config = new Xtdb.Config();
        config.setServer(serverConfig);
        try (var node = Xtdb.openNode(config);
             var connection =
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
}
