package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.MatrixCursor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by priyanka on 06/04/16.
 */
public class Message implements Serializable{
    String tag;
    String origPort;
    String mulPort;
    LinkedList<String> activePorts;
    LinkedList<String> multicast;

    HashMap<String, String> values =new HashMap<String, String>();
    String filename;
}
