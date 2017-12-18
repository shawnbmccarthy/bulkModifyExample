package org.mdb.bulk;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class Main {
    public static void main(String []args){
        MongoClientURI uri = new MongoClientURI("mongodb://localhost:27017/demo");
        MongoClient clnt = new MongoClient(uri);
        BulkModify b = new BulkModify(clnt.getDatabase(uri.getDatabase()));
        b.runBulkModify();
    }
}
