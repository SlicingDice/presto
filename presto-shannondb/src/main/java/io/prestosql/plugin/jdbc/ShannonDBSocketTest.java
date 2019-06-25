package io.prestosql.plugin.jdbc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ShannonDBSocketTest
{
    public static void main(String[] args)
    {
        try {
            ServerSocket server = new ServerSocket(1234);
            while(true){
                Socket cliente = server.accept();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
