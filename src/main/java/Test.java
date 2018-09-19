import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

public class Test {
    public static void main(String args[]) {
        MongoClient mongoClient = new MongoClient();

        // 1. Create a Database
        System.out.println("Step 1 Create a Database");
        MongoDatabase database = mongoClient.getDatabase("SM2018");

        System.out.println("Step 2 Create Collections");
        // 2. Create Collections
        database.createCollection("APP");
        database.createCollection("PDV");
        database.createCollection("FSM");
        database.createCollection("SEM");

        System.out.println("Step 3 Create Documents");
        // 3.Create Documents
        database.getCollection("APP")
                .insertOne(createDocument("APP - Assignment 1",
                        "Workshop 1", "Joe Ho", "A" ));
        database.getCollection("PDV")
                .insertOne(createDocument("Product Definition and Validation - Assignment 1",
                        "Mileston 1", "Lu", "B" ));
        database.getCollection("SEM")
                .insertOne(createDocument("Software Engineering Management - Assignment 1",
                        "Project Ideation", "Ben", "C" ));
        database.getCollection("FSM")
                .insertOne(createDocument("Foundation of Software Management - Assignment 1",
                        "Market Research", "X", "D" ));

        System.out.println("Step 4 Update Documents");
        // 4.Update Documents
        // a.Lower grades by 1 level in each of the documents.
        // i.I.e A to B, B to C, C to D, D to Fail
        MongoIterable<String> allCollectionNames =  database.listCollectionNames();
        for(String collectionName : allCollectionNames) {
            MongoCollection<Document> collection = database.getCollection(collectionName);

            FindIterable<Document> allDocuments = collection.find();
            for( Document document : allDocuments) {
                collection.updateOne(Filters.eq("student", document.get("student")), Updates.set("grade", lowerGrade((String)document.get("grade"))));
            }
        }

        System.out.println("Step 5 Retrieve Documents");
        //5. Retrieve Documents
        //Print all the documents having grade higher than Fail.
        for(String collectionName : allCollectionNames) {
            MongoCollection<Document> collection = database.getCollection(collectionName);

            FindIterable<Document> allDocuments = collection.find();
            for( Document document : allDocuments) {
                String grade = (String)document.get("grade");
                if(!grade.equals("Fail")) {
                    System.out.println("Student: "+ document.getString("student") + ", Grade: "+document.getString("grade"));
                }
            }
        }

        System.out.println("Step 6 Delete Documents");
        //6. Delete Documents
        //Delete the document from SEM collection.
        for(String collectionName : allCollectionNames) {
            MongoCollection<Document> collection = database.getCollection(collectionName);

            FindIterable<Document> allDocuments = collection.find();
            for( Document document : allDocuments) {
                collection.deleteOne(Filters.eq("student", document.get("student")));
            }
        }

        System.out.println("Step 7 Retrieve Documents");
        //7. Retrieve Documents
        // Print all the documents here : from all the collections.
        for(String collectionName : allCollectionNames) {
            System.out.println("Data of Collection: "+collectionName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            FindIterable<Document> allDocuments = collection.find();
            for( Document document : allDocuments) {
                System.out.println("Student: "+ document.getString("student") + ", Grade: "+document.getString("grade"));
            }
        }

        System.out.println("Step 8 List Collections");
        //8. List Collections
        // Print all the collections present in SM2018.
        for(String collectionName : allCollectionNames) {
            System.out.println("Collection name: "+collectionName);

        }

        System.out.println("Step 9 Drop Collections");
        //9. Drop Collections
        // Drop SEM and FSM from SM2018.
        database.getCollection("SEM").drop();
        database.getCollection("FSM").drop();

        System.out.println("Step 10 List Collections");
        //10. List Collections
        // Print all the collections present in SM2018.
        for(String collectionName : allCollectionNames) {
            System.out.println("Collection name: "+collectionName);
        }

    }

    public static Document createDocument(String title, String description, String student, String grade) {
        Document result = new Document();
        result.append("title", title);
        result.append("description", description);
        result.append("student", student);
        result.append("grade", grade);
        return result;
    }

    public static String lowerGrade(String grade) {
        if(grade.equals("A")) { return "B";}
        if(grade.equals("B")) { return "C";}
        if(grade.equals("C")) { return "D";}
        return "Fail";
    }
}
