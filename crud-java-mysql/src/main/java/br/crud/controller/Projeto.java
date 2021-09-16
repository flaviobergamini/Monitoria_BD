package br.crud.controller;

import java.sql.*;

public class Projeto {
    
    public static void main(String[] args) {
        Database database = new Database();
        database.connect();
        User user = new User("Flávio", "111.111.111-11");
        User user1 = new User("Fernando", "222.222.222-22");
        User user2 = new User("Vânia", "333.333.333-33");
        database.insertUser(user);
        database.insertUser(user1);
        database.insertUser(user2);

        database.researchUser();
        System.out.println("+#################CPF#################+");
        database.researchUserCpf("111.111.111-11");
        System.out.println("#####################################");
        database.updateUser(1, "Flavinho");

        database.researchUser();

        System.out.println("#####################################");
        database.deleteUser(1);
        database.researchUser();
    }
}
