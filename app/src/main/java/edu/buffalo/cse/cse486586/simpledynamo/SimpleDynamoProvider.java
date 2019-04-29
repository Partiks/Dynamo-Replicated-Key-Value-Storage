package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	//Parth Patel. UB Person name/number: parthras/50290764

	//partiks start variable declarations
	static String P_TAG="PartiksTag";
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String NODE_FIELD = "node";
	private static final String VERSION_FIELD = "version";
	static int[] connected_sieve = {1,1,1,1,1};
	static ArrayList<String> remotePorts = new ArrayList<String>();
	static ArrayList<String> hashed_nodes = new ArrayList<String>();
	ArrayList<Message> msgs = new ArrayList<Message>(); //msgs has the latest copy of the message
	ArrayList<Message> stale_msgs = new ArrayList<Message>(); //older versions of the message
	String portStr="";
	String myPort="";
	static final int SERVER_PORT = 10000;
	int myIndex=-1;
	String node_id;
	public static void setRemotePorts(ArrayList<String> remotePorts) { SimpleDynamoProvider.remotePorts = remotePorts;}
	public static ArrayList<String> getRemotePorts() { return remotePorts;}
	public static ArrayList<String> getHashed_nodes() { return hashed_nodes;}
	public static void setHashed_nodes(ArrayList<String> hashed_nodes) { SimpleDynamoProvider.hashed_nodes = hashed_nodes;}
	//partiks end variable declarations

	@Override
	public boolean onCreate() {
		Log.e(P_TAG, "Called ONCREATE from SimpleDhtProvider");

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		remotePorts.add(myPort);

		try {
			remotePorts.add("11124");
			remotePorts.add("11112");
			remotePorts.add("11108");
			remotePorts.add("11116");
			remotePorts.add("11120");

			hashed_nodes.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
			hashed_nodes.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
			hashed_nodes.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
			hashed_nodes.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
			hashed_nodes.add("c25ddd596aa7c81fa12378fa725f706d54325d12");
			node_id = genHash(portStr);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.e(P_TAG, "SERVER " + portStr + " / " + myPort + " got node_id: "+ node_id);
		msgs.removeAll(msgs);
		if(portStr.equals("5562")){ myIndex=0; } else if(portStr.equals("5556")){ myIndex=1; } else if(portStr.equals("5554")){ myIndex=2; } else if(portStr.equals("5558")){ myIndex=3; } else if(portStr.equals("5560")){ myIndex=4; }
		Log.e(P_TAG, "SERVER: TRYING TO CREATE SERVER SOCKET - " + SERVER_PORT + " " + myPort);
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		new SimpleDynamoProvider.ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		/*for (int i : connected_sieve){
			connected_sieve[i]=0;
		} */

		//end partiks setup
		return false;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.e(P_TAG, "Called INSERT from SimpleDhtProvider "+ values + " on node "+ myPort);

		//partiks code start
		//References:
		// https://stackoverflow.com/questions/10576930/trying-to-check-if-a-file-exists-in-internal-storage
		// https://stackoverflow.com/questions/3554722/how-to-delete-internal-storage-file-in-android
		// replication degree is 3 (2 successor nodes should have the same key) Reader and Writer Quorum size should be 2

		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String assigned_node = values.getAsString("node");
		String version = values.getAsString("version");

		if(assigned_node == null){ // new message inserted by grader

			String msgToSend = "INSERT_MSG" +","+ values.getAsString("key") + ","+ values.getAsString("value") + "," +myIndex + "," + myPort;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		}
		else{ //msg is assigned a node and is replicated to required nodes
			// destination node is selected till now, now time to store the message

			msgs.add( new Message(key, value, assigned_node, Integer.parseInt(version)) );
			Log.e(P_TAG, "---- NEW KEY "+ values.getAsString("key"));
			Log.e(P_TAG, "---- NEW VALUE "+ values.getAsString("value"));
			Log.e(P_TAG, "---- NEW ASSIGNED NODE:  "+ assigned_node);
			Log.e(P_TAG, "---- NEW VERSION:  "+ values.getAsString("version"));
		}

		//partiks code end
		return uri;
	}


	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> partiks ServerTask and ClientTask code from earlier PAs

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Iterator<Message> itr;
			Socket socket = null;

			//reference for Java Socket API code: https://www.geeksforgeeks.org/socket-programming-in-java/
			//reference for improved Java Socket API code: https://www.baeldung.com/a-guide-to-java-sockets

			while (true) {
				try {
					socket = serverSocket.accept();
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String temp;

					while ((temp = in.readLine()) != null) {
						Log.e(P_TAG, "SERVER READ A LINE:  = " + temp);
						String msg_string[] = temp.split(",");

						if ("NODE_REJOIN".equals(msg_string[0])) {

							Log.e(P_TAG, "Node rejoin request from: " + msg_string[1] + " ");
							remotePorts.add(msg_string[1]);
							int re_node = Integer.parseInt(msg_string[1]);
							if(re_node == 11112){ //avd 1
								connected_sieve[1] = 1;
							}else if(re_node == 11124){ //avd avd 4
								connected_sieve[0] = 1;
							}
							else if(re_node == 11120){ //avd 3
								connected_sieve[4] = 1;
							}
							else if(re_node == 11116){ //avd 2
								connected_sieve[3] = 1;
							}else { Log.e(P_TAG, "SHOULD NEVER HAVE REACHED HERE! NODE REJOINING ELSE"); }
							break;

						}else if("INSERT_MSG".equals(msg_string[0])){
							Log.e(P_TAG, " SERVER GOT NEW MESSAGE KEY = " + msg_string[1] + " value = " + msg_string[2]);

							//TODO: use assigned node function here in case of failed_node
							//key = msg_string[1], value = msg_string[2]
							// msg_string here = INSERT_MSG, KEY, VALUE, assigned_node, replication_node 1, replication node 2
							int msg_found_flag =0;
							int curr_version=-4;
							Message m;
							for(int i=0; i<msgs.size(); i++){
								if(msgs.get(i).getKey().equals(msg_string[1])){
									//we only need to update the version number and message
									msg_found_flag=1;
									m = msgs.get(i);
									stale_msgs.add(m);
									curr_version = m.getVersion();
									msgs.set(i, new Message(m.getKey(), msg_string[2], m.getAssignedNode(), curr_version+1));
								}
							}

							//new message, doesn't exist in msgs queue
							if(msg_found_flag == 0){
								curr_version=1;
								msgs.add( new Message(msg_string[1], msg_string[2], myPort, curr_version) );
							}
							// version is assigned to the message now time to store the message

							//Log.e(P_TAG, "STORING MESSAGE OF " + assigned_node + " key = " + c_msg[1]);
							Uri.Builder uriBuilder = new Uri.Builder();
							uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
							uriBuilder.scheme("content");
							Uri uri = uriBuilder.build();

							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, msg_string[1]);
							cv.put(VALUE_FIELD, msg_string[2]);
							cv.put(NODE_FIELD, msg_string[3]);
							cv.put(VERSION_FIELD, curr_version);

							insert(uri, cv);

						} // end of else if NAVO_MSG

						else if("QUERY_KEY".equals(msg_string[0])){
							String target_node = msg_string[2]; //11108 format
							String key = msg_string[1];
							String msgToSend=null;
							Log.e(P_TAG, "NEW TARGET NODE TO SEND CURSOR: " + target_node);
							for(int i=0; i<msgs.size(); i++){
								if(msgs.get(i).getKey().equals(key)){
									Message m = msgs.get(i);
									msgToSend = m.getKey() + "," + m.getMessage() + "," + m.getVersion();
									out.println(msgToSend);
								}
							}
							break;

							//returning cursor to target_node

						} //end of else if QUERY_KEY
						else if("GIMME_ALL".equals(msg_string[0])){
							String response_msg="";
							itr = msgs.listIterator();
							while(itr.hasNext()) {
								Message m2 = itr.next();
								if(m2.getAssignedNode().equals(myPort)){
									response_msg = response_msg + m2.getKey() + "," + m2.getMessage()+"_";
								}
							}
							out.println(response_msg);
							break;


						} //end of else if QUERY_KEY
						else if ("AAI_GAYU".equals(temp)) {
							break;
						} else {
							Log.e(P_TAG, "WEIRD SERVER ENTERED LAST ELSE with msg: " + temp);
						}
					}
					out.println("SERVER_AAI_GAYU");
					in.close();
					out.close();
					socket.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, String, MatrixCursor> {
		Socket socket;
		PrintWriter out;
		BufferedReader in;

		@Override
		protected MatrixCursor doInBackground(String... msgs2) {
			String[] c_msgs = msgs2[0].split(",");
			MatrixCursor m1 =null;
			Log.e(P_TAG, "Client doinBackground: C_MSGS first string: " + c_msgs[0] + " msgs: " + msgs + " msgs types: " + msgs.getClass().getName() + " - " + msgs.getClass().getSimpleName());
			try {

				if(c_msgs[0].equals("INSERT_MSG")){
					// 0 = insert_msg, 1 = key, 2 = value, 3 = myIndex, 4 = myPort (11108)
					//now calculating the assigned_node and sending it the message
					String node_response = findAssignedNode(c_msgs[1]);
					String[] selected_nodes = node_response.split(",");
					String msgToSend = "INSERT_MSG" + "," + c_msgs[1] + "," + c_msgs[2] + "," + selected_nodes[0] + "," + selected_nodes[1] + "," + selected_nodes[2];
					for (int i=0; i<3 ; i++){ //send message to assigned node and 2 replicated nodes
						Log.e(P_TAG, "Client sending and replicating key = " + c_msgs[1] + " value = " + c_msgs[2] + " to server = " + selected_nodes[i]);
						socket = new Socket(InetAddress.getByAddress( new byte[]{10, 0, 2, 2}), Integer.parseInt(selected_nodes[i]) );
						Log.e(P_TAG, "NEW MESSAGE CLIENT TASK: " + msgs2[0]);
						out = new PrintWriter(socket.getOutputStream(), true);
						in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						// msgToSend = INSERT_MSG, KEY, VALUE, assigned_node, replication_node 1, replication node 2
						out.println(msgToSend);
						out.println("AAI_GAYU");
						String temp;
						while ((temp = in.readLine()) != null) {
							if ("SERVER_AAI_GAYU".equals(temp)) {
								Log.e(P_TAG, "CLIENT SUCCESSFULLY SENT MSG TO " + remotePorts.get(i) + " REMOTEPORT SIZE = " + remotePorts.size() + " sending msg " + c_msgs[0] ); //+ " loop iteration " + i);
								break;
							}
						}
					}

				} // end of INSERT_MSG section
				else if(c_msgs[0].equals("QUERY_KEY")){
					String node_response = findAssignedNode(c_msgs[1]);
					String[] selected_nodes = node_response.split(",");
					String msgToSend = "QUERY_KEY" + "," + c_msgs[1] + "," + myPort + "," + selected_nodes[0] + "," + selected_nodes[1] + "," + selected_nodes[2];
					String resp_values[] = new String[3];
					int versions[]={0,0,0};
					for (int i=0; i<3 ; i++){ //send message to assigned node and 2 replicated nodes
						Log.e(P_TAG, "Client querying for key = " + c_msgs[1] + " to server = " + selected_nodes[i]);
						socket = new Socket(InetAddress.getByAddress( new byte[]{10, 0, 2, 2}), Integer.parseInt(selected_nodes[i]) );
						Log.e(P_TAG, "NEW MESSAGE CLIENT TASK: " + msgs2[0]);
						out = new PrintWriter(socket.getOutputStream(), true);
						in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						// msgToSend = QUERY, KEY, VALUE, assigned_node, replication_node 1, replication node 2
						out.println(msgToSend);
						out.println("AAI_GAYU");
						String temp;
						String query_response = in.readLine();

						while ((temp = in.readLine()) != null) {
							if ("SERVER_AAI_GAYU".equals(temp)) {
								Log.e(P_TAG, "CLIENT SUCCESSFULLY SENT MSG TO " + remotePorts.get(i) + " REMOTEPORT SIZE = " + remotePorts.size() + " sending msg " + c_msgs[0] ); //+ " loop iteration " + i);
								break;
							}
						}
						Log.e(P_TAG, "Client got response for key = " + c_msgs[1] + " RESP: " + query_response);
						String[] resp = query_response.split(",");
						// key = resp[0], value = resp[1], version = resp[2]
						versions[i] = Integer.parseInt(resp[2]);
						resp_values[i] = resp[1];
					}
					int max=0;
					for(int j=1; j<3; j++){
						if(versions[max] < versions[j]){
							max = j;
						}
					}
					Log.e(P_TAG, "Client picking up the latest version for key = " + c_msgs[1] + " version = " + versions[max] + " values = " + resp_values[max]);
					String[] cols = {"key","value"};
					MatrixCursor m2 = new MatrixCursor(cols, 1);
					String[] value = {c_msgs[1], resp_values[max]}; //key, value (of highest version number)
					Log.e(P_TAG, "CURSOR KEY_VALUE PAIR: "+ value[0] + ", " + value[1]);
					m2.addRow(value);
					return m2;


				}
				else if(c_msgs[0].equals("GIMME_ALL")){
					String msgToSend = c_msgs[0];
					String responses=null;
					for( int i=0; i<remotePorts.size(); i++){
						Log.e(P_TAG, "Client querying for key = " + c_msgs[1] + " to server = " + remotePorts.get(i));
						socket = new Socket(InetAddress.getByAddress( new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePorts.get(i)) );
						Log.e(P_TAG, "NEW MESSAGE CLIENT TASK: " + msgs2[0]);
						out = new PrintWriter(socket.getOutputStream(), true);
						in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						// msgToSend = QUERY, KEY, VALUE, assigned_node, replication_node 1, replication node 2
						out.println(msgToSend);
						out.println("AAI_GAYU");
						String temp;
						responses = responses + in.readLine();

						while ((temp = in.readLine()) != null) {
							if ("SERVER_AAI_GAYU".equals(temp)) {
								Log.e(P_TAG, "CLIENT SUCCESSFULLY SENT MSG TO " + remotePorts.get(i) + " REMOTEPORT SIZE = " + remotePorts.size() + " sending msg " + c_msgs[0] ); //+ " loop iteration " + i);
								break;
							}
						}
					}
					Log.e(P_TAG, "MEGA RESPONSES = " + responses);
					String[] pairs = responses.split("_");
					String[] cols = {"key","value"};
					MatrixCursor m2 = new MatrixCursor(cols, 1);
					for(int i=0; i<pairs.length; i++){
						String[] message = pairs[i].split(",");
						String[] value = {message[0], message[1]}; //key, value (of highest version number)
						Log.e(P_TAG, "CURSOR KEY_VALUE PAIR: "+ value[0] + ", " + value[1]);
						m2.addRow(value);
					}
					return m2;
				} //end of else if navo_msg
				else{
					Log.e(P_TAG, "????????????    THIS SHOULD NEVER COME! CLIENT TASK LAST ELSE ???????????????????? ");
				}
				out.flush();
				//partiks code end
				out.close();
				in.close();
				socket.close();

			} catch (SocketTimeoutException ste) {
				ste.printStackTrace();
			} catch (IOException e) {
				Log.e(P_TAG, "WHY IT COME HERE THOUGH ????");
				Log.e(P_TAG, "ClientTask socket IOException");
				myPort="11108";
				Log.e(P_TAG, ">>>>>>>>>>>>>> EXCEPTION >>>>>> SELF PROCLAIMED SERVER CHANGED <<<<<<<<<<<<<<<<<<<<<<<<<<<");
				String msgReceived="";
				//publishProgress(msgReceived);
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}

			return m1;
		}

	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> partiks ServerTask ClientTask code end

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		Log.e(P_TAG, "Called QUERY " + selection + " from SimpleDhtProvider " + myPort);
		ListIterator<Message> itr;
		if(!selection.equals("*") && !selection.equals("@")){ // queried with a specific key
			Log.e(P_TAG, "KEY PART OF IF ENTERED !");
			String msgToSend = "QUERY_KEY" + "," + selection;
			AsyncTask<String, String, MatrixCursor> as = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
			Log.e(P_TAG, "AFTER CLIENT TASK RETURNED CURSOR FOR KEY = "+ selection+" , ELSE PART IN QUERY OF OTHER NODES. Current node = " + portStr + " myPort = " + myPort);
			try {
				MatrixCursor mat2 = as.get();
				mat2.moveToFirst();
				Log.e(P_TAG, "FINAL ANSWER COLUMN COUNT = " + mat2.getColumnCount() + " " + mat2.getString(0) );

				return mat2;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}


		}else if(selection.equals("@")){
			itr = msgs.listIterator();
			String[] cols = {"key","value"};
			MatrixCursor m = new MatrixCursor(cols, 1);
			while(itr.hasNext()){
				Message m2 = itr.next();
				//Log.e(P_TAG, "m2 Key = " + m2.getKey() + " m2 msg = " + m2.getMessage() + "m2 assigned node = " + m2.getAssignedNode());
				if(m2.getAssignedNode().equals( myPort )){
					Log.e(P_TAG, "@@ 1 - MSG KEY: " + m2.getKey() + " Message: " + m2.getMessage() + " found for node: ");
					String[] value = {m2.getKey(), m2.getMessage()};
					m.addRow(value);
				}
			}
			return m;
		}

		else if(selection.equals("*")){
			Log.e(P_TAG, "CORRECTLY ENTERED * PART OF QUERY!!");
			String msgToSend = "GIMME_ALL" + "," + myPort;
			AsyncTask<String, String, MatrixCursor> as = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
			Log.e(P_TAG, "AFTER CLIENT TASK RETURNED CURSOR FOR KEY = "+ selection+" , ELSE PART IN QUERY OF OTHER NODES. Current node = " + portStr + " myPort = " + myPort);
			try {
				MatrixCursor mat2 = as.get();
				mat2.moveToFirst();
				Log.e(P_TAG, "FINAL ANSWER COLUMN COUNT = " + mat2.getColumnCount() + " " + mat2.getString(0) );

				return mat2;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		} // end of "*" else if
		else{
			Log.e(P_TAG, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			Log.e(P_TAG, "F");
			Log.e(P_TAG, "ERROR ! QUERY LAST ELSE SHOULD NEVER REACH HERE, selection = "+selection);
			Log.e(P_TAG, "F");
			Log.e(P_TAG, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			return null;
		}
		return null;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.e(P_TAG, "Called DELETE from SimpleDhtProvider with key/filename: " + selection);
		String filename = selection;
		if(selection.equals("*")){ //TODO: implement delete * logic
			msgs.removeAll(msgs);
		}else if(selection.equals("@")){
			msgs.removeAll(msgs);
			/*int m_index=-1;
			for(Message m: msgs){
				if(m.getAssignedNode().equals(portStr)){
					m_index = msgs.indexOf(m);
					msgs.remove(m_index);
				}
			} */
		}else{
			int m_index=-1;
			Message m2 = null;
			for(Message m : msgs){
				Log.e(P_TAG, "FINDING TO DELETE: " + m.getKey() + " - " + filename);
				if(m.getKey().equals(filename)){
					Log.e(P_TAG, "FOUND TO DELETE: " + m.getKey() + " - " + filename);
					m_index = msgs.indexOf(m);
					m2 = m;
					break;
				}
			}

			if(m_index != -1){
				Log.e(P_TAG, "TRIED AND DELETED: "+ m_index + " KEY: " + msgs.get(m_index).getKey());
				//msgs.remove(m_index);
				msgs.remove(m2);
				msgs.

				String path = getContext().getFilesDir().getAbsolutePath() + "/" + filename;
				File f = new File(path);
				if(f.exists()){
					f.delete();
					return 0;
				}
			}
			return 0;
		}
		return 0;
	}


	public String findAssignedNode(String key){
		//String[] c_msg = m.split(",");

		String msg_key_hash = null;
		String assigned_node = null;
		String rep_node1 = null;
		String rep_node2 = null;

		//REFERENCE FOR FORMAT: String msgToSend = "navo_msg" +","+ values.getAsString("key") + ","+ values.getAsString("value");
		try {   msg_key_hash = genHash(key);  } catch (NoSuchAlgorithmException e) {  e.printStackTrace();  }
		int found_flag = 0;

		//first checking the edge case of wether the key belongs to first node i.e. 5554 (greater than the last node as well as all keys that are smaller than the first node
		//get the last node connected according to sequence: avd 1,4,3,2
		int last_node=-1;
		int first_node=-1;
		int x=0;
		while(x<5){
			//Log.e(P_TAG, "LOOP 1" + " x = " + x + " connection status = "+ connected_sieve[x]);
			if(connected_sieve[x] == 1){
				first_node=x;
				break;
			}
			x++;
		}
		x=4; found_flag = 0;
		while(x>0){
			//Log.e(P_TAG, "LOOP 2" + " x = " + x + " connection status = "+ connected_sieve[x]);
			if(connected_sieve[x] == 1){
				last_node=x;
				break;
			}
			x--;
		}


		Log.e(P_TAG, " MSG_KEY_HASH = " + msg_key_hash + "KEY: " + key+" last_node = " + last_node);
		if(msg_key_hash.compareTo(hashed_nodes.get(last_node)) > 0 && msg_key_hash.compareTo(hashed_nodes.get(first_node)) > 0){
			if(first_node == 0){
				//assigned_node = "5562"; rep_node1 = "5556"; rep_node2="";
				assigned_node = "11124"; rep_node1 = "11112"; rep_node2="11108";
				found_flag = 1;
				Log.e(P_TAG, "1 - CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
				//break;
			}else if(first_node == 1){
				assigned_node = "11112"; rep_node1="11108"; rep_node2 = "11116";
				found_flag = 1;
				Log.e(P_TAG, "1 - CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
				//break;
			}else if(first_node == 2){
				assigned_node = "11108"; rep_node1="11116"; rep_node2 = "11120";
				found_flag = 1;
				Log.e(P_TAG, "1 - CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
				//break;
			}else if(first_node == 3){
				assigned_node = "11116"; rep_node1="11120"; rep_node2 = "11124";
				found_flag = 1;
				Log.e(P_TAG, "1 - CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
				//break;
			}else if (first_node == 4){
				assigned_node = "11120"; rep_node1="11124"; rep_node2 = "11112";
				found_flag = 1;
				Log.e(P_TAG, "1 - CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
				//break;
			}
		}

		if(found_flag == 0){
			//iteratively find the node which will be responsible for the key.

			//for ( int i=0; i<(remotePorts.size() + x) ; i++ ){
			for ( int i=0; i<5 ; i++ ){
				//Log.e(P_TAG, "COMPARING = " + msg_key_hash + " to " +hashed_nodes.get(i)+ " connection status= " + connected_sieve[i] + " i= " + i);
				if(connected_sieve[i] == 0){
					continue;
				}
				if( msg_key_hash.compareTo(hashed_nodes.get(i)) <= 0 && connected_sieve[i] == 1){
					//if( msg_key_hash.compareTo(hashed_nodes.get(i-1)) > 0 ){ //edge case remaining to check if the predecessor node has been connected or not
					if(i == 0){
						assigned_node = "11124"; rep_node1 = "11112"; rep_node2="11108";
						Log.e(P_TAG, "CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
						break;
					}else if(i == 1){
						assigned_node = "11112"; rep_node1="11108"; rep_node2 = "11116";
						Log.e(P_TAG, "CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
						break;
					}else if(i == 2){
						assigned_node = "11108"; rep_node1="11116"; rep_node2 = "11120";
						Log.e(P_TAG, "CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
						break;
					}else if(i == 3){
						assigned_node = "11116"; rep_node1="11120"; rep_node2 = "11124";
						Log.e(P_TAG, "CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
						break;
					}else if (i == 4){
						assigned_node = "11120"; rep_node1="11124"; rep_node2 = "11112";
						Log.e(P_TAG, "CHOOSING DESTINATION NODE: " + assigned_node + " for hashed key: " + msg_key_hash + " original key: " + key + ">>>>>>>>>>>>>>>>>>>>>>>");
						break;
					}else{
						Log.e(P_TAG, "WEIRD, ASSIGNED NODE FINDING LOOP ELSE HIT CAME OUT !!!");
					}
					//}

				}
			}  //end of for loop
		} //end of else or found_flag if

		String responseStr = assigned_node + "," + rep_node1 + ","+ rep_node2;

		return responseStr;
	}



	private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		Log.e(P_TAG, "Called UPDATE from SimpleDhtProvider");
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		Log.e(P_TAG, "Called GET_TYPE from SimpleDhtProvider");
		return null;
	}

}
