package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyProtocol extends VersionedProtocol{
	public static long versionID = 1234L;
	public String hello(String name);
}
