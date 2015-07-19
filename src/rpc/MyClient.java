package rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {

	public static void main(String[] args) throws Exception {
		MyProtocol client = RPC.getProxy(MyProtocol.class, 1234L, new InetSocketAddress("localhost",1111), new Configuration());
		String result = client.hello("world");
		System.out.println(result);
		//RPC.stopProxy(client);
	}    
   
}
