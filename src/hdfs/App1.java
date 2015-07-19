package hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class App1 {

	public static void main(String[] args) throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://crxy1:9000/"), new Configuration());
		//fileSystem.mkdirs(new Path("/dir1"));
		//fileSystem.mkdirs(new Path("/dir2"),new FsPermission("111"));
		
		/*FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus);
		}*/
		
		/*FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			if(!fileStatus.isDirectory()){
				BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
				for (BlockLocation blockLocation : fileBlockLocations) {
					String[] hosts = blockLocation.getHosts();
					for (String host : hosts) {
						System.out.println("HOST NAME: "+host);
					}
				}
			}
		}*/
		
		//fileSystem.delete(new Path("/dir1"), true);
		
		String work = fileSystem.getWorkingDirectory().toString();
		System.out.println(work);
	}

}
