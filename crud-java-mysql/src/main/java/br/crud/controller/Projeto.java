package br.crud.controller;

import java.sql.*;

public class Projeto {

    static final String user = "root";
    static final String password = "/MS-DOSV.6.22b";
    static final String database = "exercicio";
    static final String url = "jdbc:mysql://localhost:3306/" + database + "?useTimezone=true&serverTimezone=UTC&useSSL=false";

    public static void main(String[] args) {
        Database database = new Database();
        database.connect();
        User user = new User("Flavio", "111.111.111-11");
        //database.insertUser(user);
        database.researchUser();
    }
}
