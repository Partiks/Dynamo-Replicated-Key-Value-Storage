package edu.buffalo.cse.cse486586.simpledynamo;

public class Message {
    String key;
    String message;
    String assigned_node;

    Message(String k, String msg, String assigned_node){
        this.key = k;
        this.message = msg;
        this.assigned_node = assigned_node;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getAssignedNode(){ return assigned_node;  }

    public static java.util.Comparator<Message> id = new java.util.Comparator<Message>(){
        public int compare(Message m1, Message m2){
            String m1_key = m1.getKey();
            String m2_key = m2.getKey();

            return m1_key.compareTo(m2_key);
        }
    };
}