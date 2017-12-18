package org.mdb.bulk;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.Block;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonInt64;
import org.bson.Document;

public class BulkModify {
    /*
     * change these as needed
     */
    private static final String SOURCE = "acct";
    private static final String LOOKUP = "acctMaster";
    private static final String TARGET = "acctTarget";

    private final MongoDatabase mDb;
    private final String mSrcColl;
    private final String mTargetColl;
    private final String mLookupColl;


    public BulkModify(MongoDatabase db, String src, String target, String lookup){
        mDb = db;
        mSrcColl = src;
        mTargetColl = target;
        mLookupColl = lookup;
    }

    public BulkModify(MongoDatabase db){
        this(db, SOURCE, TARGET, LOOKUP);
    }

    /*
     * returns:
     * {$match: {UserID: uid}}
     */
    private Document match(String uid){
        return new Document("$match", new Document("UserID", uid));
    }

    /*
     * returns:
     * {$lookup: {from: from, as: as, localField: local, foreignField: foreign}}
     */
    private Document lookup(String from, String as, String local, String foreign){
        return new Document(
                "$lookup",
                new Document("from", from)
                    .append("as", as)
                    .append("localField", local)
                    .append("foreignField", foreign)
        );
    }

    /*
     * returns:
     * {$unwind: '$' + target}
     */
    private Document unwind(String target){
        return new Document("$unwind", "$" + target);
    }

    /*
     * returns:
     * { $group: {
     *    _id: {UserID: '$UserID', AccountNumber: '$AccountNumber'},
     *    RegRepNumbers: {$push: '$RegRepNumber'},
     *    OIPs: {$push: '$OIP'}
     *   }
     * }
     */
    private Document group(){
        return new Document(
            "$group", new Document(
                "_id", new Document("UserID", "$UserID").append("AccountNumber", "$AccountNumber")
            )
                .append("RegRepNumbers", new Document("$push", "$RegRepNumber"))
                .append("OIPs", new Document("$push", "$OIP"))
        );
    }

    /*
     * returns:
     * { $project: {
     *     _id: 0,
     *     UserID: '$_id.UserID',
     *     AccountNumber: '$_id.AccountNumber',
     *     RegRepNumbers: 1,
     *     OIPs: 1,
     *     DateOpen: '$target.DateOpen',
     *     DateClosed: '$target.DateClosed',
     *     TitleAddress1: '$target.TitleAddress1',
     *     PhoneNumber: {$subStrBytes: ['$target.PhoneNumber', 0, 10]}
     *   }
     * }
     */
    private Document project(String target){
        BsonArray arr = new BsonArray();
        arr.add(new BsonString("$" + target + ".PhoneNumber"));
        arr.add(new BsonInt64(0));
        arr.add(new BsonInt64(10));

        return new Document(
                "$project", new Document("_id", 0)
                .append("UserID", "$_id.UserID")
                .append("AccountNumber", "$_id.AccountNumber")
                .append("RegRepNumbers", 1)
                .append("OIPs", 1)
                .append("DateOpen", "$" + target + ".DateOpen")
                .append("DateClosed", "$" + target + "target.DateClosed")
                .append("TitleAddress1", "$" + target + ".TitleAddress1")
                .append("PhoneNumber", new Document("$subStrBytes", arr))
        );
    }

    /*
     * db.coll.aggregate([
     *   {$match: {'UserID': 'xxxx2345'}},
     *   {$lookup: {
     *     from: 'target',
     *     as: 'target',
     *     localField: 'AccountNumber',
     *     foreignField: 'AccountNumber'
     *   }},
     *   {$unwind: '$target'},
     *   {$group: {
     *     _id: {UserID: '$UserID', AccountNumber: '$AccountNumber'},
     *     RegRepNumbers: {$push: '$RegRepNumber'}
     *     OIPs: {$push: '$OIP'}
     *   }},
     *   {$project: {
     *     _id: 0,
     *     UserID: '$_id.UserID',
     *     AccountNumber: '$_id.AccountNumber',
     *     RegRepNumbers: '$RegRepNumbers',
     *     OIPs: '$OIPs',
     *     DateOpen: '$target.DateOpen',
     *     DateClosed: '$target.DateClosed',
     *     TitleAddress1: '$target.TitleAddress1',
     *     PhoneNumber: {$subStrBytes: ['$target.PhoneNumber', 0, 10]}
     *   }}
     * ]);
     */
    public void runBulkModify(){
        DistinctIterable<String> results;
        final List<Document> docs = new ArrayList<Document>();

        Block<Document> insertMany = new Block<Document>(){
            public void apply(final Document doc){
                System.out.println("apply");
                docs.add(doc);
                if(docs.size() >= 1000){
                    System.out.println("Inserting 1000 docs");
                    mDb.getCollection(mTargetColl).insertMany(docs);
                    docs.clear();
                }
            }
        };

        BsonArray arr = new BsonArray();
        arr.add(new BsonString("$" + mTargetColl + ".PhoneNumber"));
        arr.add(new BsonInt64(0));
        arr.add(new BsonInt64(10));

        System.out.println("running distinct on src: " + mSrcColl);
        results = mDb.getCollection(mSrcColl).distinct("UserID", String.class);
        for (String uid : results) {
            System.out.println("uid: " + uid);
            mDb.getCollection(mSrcColl).aggregate(
                    Arrays.asList(
                            Aggregates.match(Filters.eq("UserID", uid)),
                            Aggregates.lookup(mLookupColl, mLookupColl, "AccountNumber", "AccountNumber"),
                            Aggregates.unwind("$" +    mLookupColl),
                            Aggregates.group(
                                new Document("_id", new Document("UserID", "$UserID").append("AccountNumber", "$AccountNumber")),
                                    Accumulators.push("RegRepNumbers", "$regRepNumbers"),
                                    Accumulators.push("OIPs", "$OIP")
                            ),
                            Aggregates.project(
                                Projections.fields(
                                    Projections.excludeId(),
                                    Projections.include("RegRepNumbers", "OIPs"),
                                    Projections.computed("UserID", "$_id.UserID"),
                                    Projections.computed("AccountNumber", "$_id.AccountNumber"),
                                    Projections.computed("DateOpen", "$" + mTargetColl + ".DateOpen"),
                                    Projections.computed("DateClosed", "$" + mTargetColl + ".DateClosed"),
                                    Projections.computed("PhoneNumber", new Document("$substrBytes", arr))
                                )
                            )
                    )
            ).allowDiskUse(true).forEach(insertMany);
        }

        if(docs.size() > 0){
            System.out.println("inserting rest of documents");
            mDb.getCollection(mTargetColl).insertMany(docs);
            docs.clear();
        }
    }


}
