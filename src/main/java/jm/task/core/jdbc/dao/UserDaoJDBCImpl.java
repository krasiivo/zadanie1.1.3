package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private final static String CREATE_TABLE_SQL = """
            CREATE TABLE users (
                user_id INT PRIMARY KEY AUTO_INCREMENT,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                age INT NOT NULL
                CHECK (age > 0 AND age < 100)
            );
            """;

    private final static String DROP_TABLE_SQL = """
            DROP TABLE users
            """;

    private final static String SAVE_USER_SQL = """
            INSERT INTO users (first_name, last_name, age)
            VALUES (?, ?, ?);
            """;

    private final static String DELETE_USER_SQL = """
            DELETE FROM users
            WHERE user_id = ?
            """;

    private final static String GET_ALL_USERS_SQL = """
            SELECT user_id,
                   first_name,
                   last_name,
                   age
            FROM users
            """;

    private final static String DELETE_ALL_USERS_SQL = """
            DELETE FROM users
            """;

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        try (Connection connection = Util.open();
             PreparedStatement preparedStatement = connection.prepareStatement(CREATE_TABLE_SQL)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.open();
             PreparedStatement preparedStatement = connection.prepareStatement(DROP_TABLE_SQL)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.open();
             PreparedStatement preparedStatement = connection.prepareStatement(SAVE_USER_SQL)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.open();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_USER_SQL)) {
            preparedStatement.setLong(1, id);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Connection connection = Util.open();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_ALL_USERS_SQL)) {
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                users.add(buildUser(resultSet));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    private User buildUser(ResultSet resultSet) throws SQLException {
        String firstName = resultSet.getString("first_name");
        String lastName = resultSet.getString("last_name");
        byte age = resultSet.getByte("age");
        long userId = resultSet.getLong("user_id");

        User user = new User(firstName, lastName, age);
        user.setId(userId);

        return user;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.open();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_ALL_USERS_SQL)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

