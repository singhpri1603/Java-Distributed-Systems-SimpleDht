package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.opengl.Matrix;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    String TAG="SimpleDHT";
    String node_id;
    String Port;
    String myPort;
    LinkedList<String> multicast=new LinkedList<String>();
    LinkedList<String> activePorts=new LinkedList<String>();
    LinkedList<String> activeNodes;
    String suc;
    String pre;
    Boolean flag;
    static final ReentrantLock lock = new ReentrantLock(true);
    static final int SERVER_PORT = 10000;

    MatrixCursor matcur;
    int count=0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        getContext().deleteFile(selection);

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = (String)values.get("key");
        String value= (String)values.get("value");
        FileOutputStream outputStream;

        try {
            String hashkey = genHash(key);

            if(activePorts.size()==1){
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            }


            else{

                if(genHash(pre).compareTo(genHash(myPort))>0){
                    Log.v("at", "first node");
                    if(hashkey.compareTo(genHash(myPort))<=0 || hashkey.compareTo(genHash(pre))>0){
                        Log.v("inserting","at first node");
                        outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                    }
                    else {
                        Log.v("sending to","successor");
                        //send to suc code
                        Message msg=new Message();
                        msg.tag="insert";
                        String k=(String)values.get("key");
                        String v=(String)values.get("value");
                        msg.values.put(k,v);
                        Log.e("suc", suc);
                        int temp=Integer.parseInt(suc);
                        temp=temp*2;
                        String tempSuc=String.valueOf(temp);
                        Log.e("tempSuc", tempSuc);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(tempSuc));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                        out.writeObject(msg);

                        socket.close();
                    }
                }

                else{
                    Log.v("at", "node "+myPort);

                    if(hashkey.compareTo(genHash(pre))>0 && hashkey.compareTo(genHash(myPort))<=0){
                        Log.v("inserting", "at node "+myPort);
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }


            else {

                        Log.v("sending to","successor");
                //send to suc code
                Message msg=new Message();
                msg.tag="insert";
//                        String k=(String)values.get("key");
//                        String v=(String)values.get("value");

                        Log.v("values",key+" "+value);
                msg.values.put(key,value);
                        Log.e("suc", suc);
                    int temp=Integer.parseInt(suc);
                    temp=temp*2;
                    String tempSuc=String.valueOf(temp);
                        Log.e("tempSuc", tempSuc);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(tempSuc));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                out.writeObject(msg);

                socket.close();
            }

        }}}catch(NoSuchAlgorithmException Ex){
            Log.e(TAG, Ex.getMessage());
        }catch (Exception e) {
            //Log.e(TAG, e.getMessage());
            Log.e(TAG, "File write failed");
        }

        Log.v("here ","here");

        Log.v("insert", values.toString());

        Log.v("all ","ok");
        return uri;
        //return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);

            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            Port = String.valueOf((Integer.parseInt(portStr) * 2));
            myPort=String.valueOf((Integer.parseInt(portStr)));
            Log.v("pri", myPort);
            Log.v("pri", Port);
            node_id=genHash(myPort);


        }catch(NoSuchAlgorithmException ex){
            Log.e("abc","abc");
        }
        catch(Exception e){
            Log.e("excptn",e.getMessage());
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");

        }

        if(myPort.equals("5554") ){
            activePorts.add(myPort);
            multicast.add(Port);
        }

        else{
            //send i am alive message to 5554
            activePorts.add(myPort);
            multicast.add(Port);
            String tag="alive";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, tag);

        }



        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        String symbol = selection;
        String[] columnNames= {"key","value"};
        MatrixCursor MC= new MatrixCursor(columnNames);
        Log.v("query1", "query1");

        if(symbol.equals("*") && activePorts.size()==1){
            symbol="@";
        }

        if(symbol.equals("@")){
            try{
                Log.v("query2", "query2");
                File file = new File( getContext().getFilesDir()+"/");
                File[] filenames=file.listFiles();
                for(File temp:filenames) {
                    Log.v("query3", "query3");
                    FileInputStream fis = new FileInputStream(temp);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                    String value = null;
                    try {
                        value = reader.readLine();
                    } catch (IOException e) {
                        Log.e("", "IO Exception!!!!!!");
                    }
                    Log.v("temp", temp.toString().split("/")[5]);

                    Log.v("value", value);
                    String[] values = {temp.toString().split("/")[5], value};
                    MC.addRow(values);

                }
            }catch(Exception e){
                Log.e(TAG, e.getMessage());
            }

            return MC;
        }

        else if(symbol.equals("*")){
            //multicast for all data

            Message msg=new Message();
            msg.tag="*";
            msg.origPort=myPort;
            count=activeNodes.size();
            String[] columns= {"key","value"};
            matcur= new MatrixCursor(columns);
            flag=true;

            try {
                String[] remotePort = multicast.toArray(new String[multicast.size()]);
                for (String s : remotePort) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(s));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                    out.writeObject(msg);

                    socket.close();
                }
            }catch(Exception e){
                Log.e(TAG, e.getMessage());
            }
            while(flag){
                continue;
            }
            return matcur;
        }
        else{

            Log.v("if not * or @","comes here");
            String filename = selection;

            File file = new File( getContext().getFilesDir()+"/");
            File[] filenames=file.listFiles();

            LinkedList<String> myfiles=new LinkedList<String>();

            for(File temp:filenames){
                myfiles.add(temp.toString().split("/")[5]);
                Log.v("file",temp.toString().split("/")[5]);
            }

            Log.v("linked list ","created");

            if(myfiles.contains(filename)) {

                Log.v("inside","if");

                File thisfile = new File( getContext().getFilesDir() + "/" + filename );

                try {
                    FileInputStream fis = new FileInputStream(thisfile);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                    String value = null;
                    try {
                        value = reader.readLine();
                    } catch (IOException e) {
                        Log.e("", "IO Exception!!!!!!");
                    }
                    String[] values = {selection, value};
                    MC.addRow(values);

                    Log.v("returning","cursor");
                    return MC;
                } catch (FileNotFoundException e) {
                    Log.e("", "file not found!!!!!!");
                }
            }

            else{

                Log.v("asking others","for file");
                Message msg=new Message();
                msg.tag="single";
                msg.origPort=myPort;
                msg.filename=selection;
                count=activeNodes.size();
                String[] columns= {"key","value"};
                matcur= new MatrixCursor(columns);
                flag=true;

                try {

                    int temp=Integer.parseInt(suc);
                    temp=temp*2;
                    String tempSuc=String.valueOf(temp);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(tempSuc));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                        out.writeObject(msg);

                        socket.close();

                }catch(Exception e){
                    Log.e(TAG, e.getMessage());
                }
                while(flag){
                    continue;
                }
                Log.v("file returned from ","others here");

                flag=true;

                return matcur;
            }

            Log.v("query", selection);
            return null;
        }

     //   return null;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs){
            if(msgs[0].equals("alive")){
                try {
                    String s = "11108";
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(s));


                    Message msg= new Message();

                    msg.tag=msgs[0];
                    msg.origPort=myPort;
                    msg.mulPort=Port;

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msg);

                    socket.close();
                    Log.v("client each ", "port client");
                }catch(UnknownHostException e){
                    Log.e(TAG, e.getMessage());
                }catch(IOException e){
                    //Log.e(TAG, e.getMessage());
                    Log.e(TAG, "do nothing");
                }catch(Exception e){
                    Log.e(TAG, e.getMessage());
                }
            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        ContentValues cv ;
        ContentResolver cr = getContext().getContentResolver();
        int counter = 0;
        //String KEY_FIELD = "key";
        //String VALUE_FIELD = "value";


        Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while(true){
                try{
                    Socket clientSocket = serverSocket.accept();
                    lock.lock();
                    ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
                    Message msg = (Message) inputStream.readObject();

                    if(msg.tag.equals("alive")){
                        Log.v("alive", msg.origPort);

                        activePorts.add(msg.origPort);

                        //Log.v("list",activePorts);
                        multicast.add(msg.mulPort);
                        msg.tag="ports";
                        msg.activePorts=activePorts;
                        msg.multicast=multicast;

                        String[] remotePort = multicast.toArray(new String[multicast.size()]);
                        for (String s : remotePort) {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(s));
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                            out.writeObject(msg);

                            socket.close();
                        }
                    }

                    else if(msg.tag.equals("ports")){

                        Log.v("active", "ports");

                        activePorts=msg.activePorts;
                        multicast=msg.multicast;
                        Iterator itr=activePorts.iterator();
                        activeNodes=new LinkedList<String>();
                        while(itr.hasNext()){
                            String port=(String)itr.next();
                            activeNodes.add(genHash(port));
                        }

                        if(activeNodes.size()==1){}


                        else{
                            Log.v("active ports size",""+activePorts.size());
                        String[] arrActivePorts = activePorts.toArray(new String[activePorts.size()]);
                        String[] arrActiveNodes = activeNodes.toArray(new String[activeNodes.size()]);
                            Log.v("active ports array size",""+arrActiveNodes.length);

                        for (int n = 0; n < arrActiveNodes.length; n++) {
                            for (int m = n+1; m < arrActiveNodes.length; m++) {
                                Log.v(""+m,""+n);
                                if ((arrActiveNodes[m].compareTo(arrActiveNodes[n])) < 0) {
                                    String swap = arrActiveNodes[m];
                                    arrActiveNodes[m] = arrActiveNodes[n];
                                    arrActiveNodes[n] = swap;
                                    swap = arrActivePorts[m];
                                    arrActivePorts[m] = arrActivePorts[n];
                                    arrActivePorts[n] = swap;
                                }
                            }
                        }

                        int loc=0;

                        for(int i=0; i<arrActiveNodes.length;i++){
                            if(arrActiveNodes[i].equals(genHash(myPort))){
                                loc=i;
                                break;
                            }
                        }

                            Log.e(myPort, ""+loc);


                        if(loc==0){
                            suc=arrActivePorts[1];
                            pre=arrActivePorts[arrActivePorts.length-1];
                        }

                        else if(loc==arrActiveNodes.length-1){
                            suc=arrActivePorts[0];
                            pre=arrActivePorts[loc-1];
                        }

                        else {
                            suc=arrActivePorts[loc+1];
                            pre=arrActivePorts[loc-1];
                        }

                            Log.e(myPort, "pre-"+pre+" suc-"+suc);

                    }}

                    else if(msg.tag.equals("insert")){

                        cv=new ContentValues();

                        Set<String> keySet=msg.values.keySet();

                        Object[] keys=keySet.toArray();

                        String k=keys[0].toString();

                        String v=msg.values.get(keys[0].toString());

                        cv.put("key", k);
                        cv.put("value", v);


                        cr.insert(mUri, cv);
                    }

                    else if(msg.tag.equals("*")){
                        Log.v("entering","* query");
                        Cursor cursor=cr.query(mUri,null,"@",null,null);

                        String k;
                        String v;
                        while(cursor.moveToNext()) {
                            int x = cursor.getColumnIndex("key");
                            int y = cursor.getColumnIndex("value");
                            k = cursor.getString(x);
                            v = cursor.getString(y);
                            Log.v("cursor values", k + " " + v);

                            msg.values.put(k, v);

                        }
                        cursor.close();

                        int temp = Integer.parseInt(msg.origPort);
                        temp = temp * 2;
                        String tempSuc = String.valueOf(temp);
                        Log.v("tempSuc", tempSuc);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(tempSuc));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                        msg.tag="query";

                        out.writeObject(msg);

                        socket.close();
                    }

                    else if(msg.tag.equals("query")){
                        Log.v("values return","from others");
                        count--;

                        Set<String> keySet=msg.values.keySet();

                        Object[] keys=keySet.toArray();

                        for(Object obkey:keys){
                            String key=obkey.toString();
                            String value=msg.values.get(key);
                            String[] values={key,value};
                            matcur.addRow(values);
                        }

                        if(count==0) {
                            flag = false;
                        }
                    }

                    else if(msg.tag.equals("single")){
                        Log.v("entering","single query");
                        String filename = msg.filename;

                        File file = new File( getContext().getFilesDir()+"/");
                        File[] filenames=file.listFiles();

                        LinkedList<String> myfiles=new LinkedList<String>();

                        for(File temp:filenames){
                            myfiles.add(temp.toString().split("/")[5]);
                        }

                        if(myfiles.contains(filename)) {

                            Log.v("if","found");

                            msg.tag="found";

                            Cursor cursor=cr.query(mUri,null,filename,null,null);
                            String k;
                            String v;
                            if(cursor.moveToFirst()) {
                                int x = cursor.getColumnIndex("key");
                                int y = cursor.getColumnIndex("value");
                                k = cursor.getString(x);
                                v = cursor.getString(y);

                            }else {
                                k = "empty";
                                v = "empty";
                            }
                            cursor.close();

                            Log.v("cursor values", k + " " + v);

                                msg.values.put(k,v);



                            int temp = Integer.parseInt(msg.origPort);
                            temp = temp * 2;
                            String tempSuc = String.valueOf(temp);
                            Log.v("tempSuc", tempSuc);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(tempSuc));
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                            out.writeObject(msg);

                            socket.close();

                        }

                        else {

                            int temp = Integer.parseInt(suc);
                            temp = temp * 2;
                            String tempSuc = String.valueOf(temp);

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(tempSuc));
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
                            out.writeObject(msg);

                            socket.close();

                        }
                    }

                    else if(msg.tag.equals("found")){
                        Log.v("found","the key");

                        Set<String> keySet=msg.values.keySet();

                        Object[] keys=keySet.toArray();

                        String k=keys[0].toString();

                        String v=msg.values.get(keys[0].toString());

                        String[] values={k,v};
                        matcur.addRow(values);
                        flag=false;
                    }


                }catch(Exception e){
                    Log.e("here", e.getMessage());
                }
                finally{
                    lock.unlock();
                }
            }

            //return null;
        }

    }

}
