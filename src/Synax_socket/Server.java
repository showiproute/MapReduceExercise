package Synax_socket;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Server {

	public static void main(String[] args) throws Exception {
		int port=8000;
		
		ServerSocket server = new ServerSocket(port);
		System.out.println("Server is waiting for the client");
		System.out.println("Server send first");
		
		Socket socket = server.accept();
		
		InputStream inputStream = socket.getInputStream();
		OutputStream outputStream = socket.getOutputStream();
		
		System.out.print("发送:");
		Scanner sc = new Scanner(System.in);
		outputStream.write(sc.nextLine().getBytes());
		
		byte[] bytes=new byte[1024];
		int len;
		
		StringBuilder sb = new StringBuilder();
		while((len=inputStream.read(bytes))!=-1) {
			sb.delete(0, sb.length());
			sb.append(new String(bytes,0,len));
			System.out.println("接收:"+sb);
			Scanner scanner = new Scanner(System.in);
			System.out.print("发送:");
			outputStream.write(scanner.nextLine().getBytes());
		}
		
		inputStream.close();
		outputStream.close();
		socket.close();
		server.close();
	}
}
