package br.crud.controller;

import java.sql.*;
import java.util.ArrayList;

public class Database {

    Connection connection;
    Statement statement;
    PreparedStatement pst;

    static final String user = "root";
    static final String password = "/MS-DOSV.6.22b";
    static final String database = "projeto";
    static final String url = "jdbc:mysql://localhost:3306/" + database + "?useTimezone=true&serverTimezone=UTC&useSSL=false";
    private boolean check = false;

    public void connect(){
        try {
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Conexão feita com sucesso: "+ connection);
        }catch (SQLException e){
            System.out.println("Erro de conexão: " + e.getMessage());
        }
    }

    public boolean insertUser(User user){
        connect();
        String sql = "INSERT INTO usuario(nome, cpf) VALUES(?, ?)";
        try {

            pst = connection.prepareStatement(sql);
            pst.setString(1, user.getNome());
            pst.setString(2,user.getCpf());
            pst.execute();
            check = true;

        } catch (SQLException e) {
            System.out.println("Erro de operação: " + e.getMessage());
            check = false;
        }
        finally {
            try{
                connection.close();
                pst.close();
            }catch (SQLException e){
                System.out.println("Erro ao fechar a conexão: " + e.getMessage());
            }
        }
        return check;
    }
}
