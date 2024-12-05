package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();
        userService.createUsersTable();

        userService.saveUser("Anton", "Kondakov", (byte)24);
        userService.saveUser("Alisa", "Petrova", (byte)25);
        userService.saveUser("Arsen", "Sabitov", (byte)22);
        userService.saveUser("Artem", "Seitov", (byte)27);

        userService.removeUserById(3);

        userService.getAllUsers();

        userService.cleanUsersTable();

        userService.dropUsersTable();
    }
}
