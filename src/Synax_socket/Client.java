package Synax_socket;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

public class Client {
	public static void main(String[] args) throws Exception{
		
		String host="127.0.0.1";
		int port=8000;
		Socket socket=new Socket(host,port);
		OutputStream outputStream = socket.getOutputStream();
		InputStream inputStream = socket.getInputStream();
		String message="connect to the server";
		
		outputStream.write(message.getBytes());
		
		byte[] bytes=new byte[1024];
		int len;
		StringBuilder sb= new StringBuilder();
		
		while((len=inputStream.read(bytes))!=-1) {
			sb.append(new String(bytes,0,len));
			System.out.println("接收:"+sb);
			Scanner scanner = new Scanner(System.in);
			System.out.println("发送:");
			outputStream.write(scanner.nextLine().getBytes());
		}
		
		System.out.println("end");
		
		inputStream.close();
		outputStream.close();
		socket.close();
	}
	
}
