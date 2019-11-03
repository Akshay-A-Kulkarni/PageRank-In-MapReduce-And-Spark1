package pr;

import java.io.*;
import java.util.ArrayList;

public class Vertex implements Serializable {
    private final Integer  Vertex_ID;
    private Double Vertex_PR;
    private ArrayList<Integer> adjVertices;

    public Vertex (int  Vertex_ID)  {
        this.Vertex_ID = Vertex_ID;
        this.adjVertices = new ArrayList<>();
    }

    public void setPR(Double input){
        this.Vertex_PR = input;
    }
    public Double getPR(){
        return this.Vertex_PR;
    }
    public ArrayList<Integer> getAdjacencyList(){
        return this.adjVertices;
    }
    public void addToAdjacencyList(int Vertex){
        this.adjVertices.add(Vertex);
    }

    public String serializeToString(){
        return "#"+this.Vertex_ID.toString()+"@"+ this.Vertex_PR.toString()+"@"+this.adjVertices.toString()+"#";
    }

    public Vertex deserializeFromString(String in){
        final String[] values  = in.replaceAll("[#]", "").split("@");
        int v = Integer.parseInt(values[0]);
        double pr = Double.parseDouble(values[1]);

        Vertex output = new Vertex(v);
        output.setPR(pr);

        final String[] adj = values[2].replaceAll("[\\[\\]]","").split(",");
        for (String s:adj) {
            output.addToAdjacencyList(Integer.parseInt(s));
        }

        return output;
    }

    @Override
    public int hashCode () {
        return this.Vertex_ID.hashCode();
    }
    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        else
            return false;
    }
    @Override
    public String toString () {
        return "Vertex Object Id: "+ this.Vertex_ID + " | Current PageRank Value: " + this.Vertex_PR + "[Note: use .serializeToString to get the full object as a parsable string]";
    }

    public static void main(String[] args) {

        Vertex v = new Vertex(1);
        v.addToAdjacencyList(2);
        v.setPR(2.595);


        String out = v.serializeToString();

        System.out.println(out);
        Vertex test = v.deserializeFromString(out);

        System.out.println(test.serializeToString());
//        try
//        {
//            //Saving of object in a file
//            FileOutputStream file = new FileOutputStream("ser_test.ser");
//            ObjectOutputStream out = new ObjectOutputStream(file);
//
//            // Method for serialization of object
//            out.writeObject(v);
//
//            out.close();
//            file.close();
//
//            System.out.println("Object has been serialized");
//
//        }
//
//        catch(IOException ex)
//        {
//            System.out.println("IOException is caught");
//        }
//
//
//        Vertex object1 = null;

        // Deserialization
//        try
//        {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream("ser_test.ser");
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            object1 = (Vertex) in.readObject();
//
//            in.close();
//            file.close();
//
//            System.out.println("Object has been deserialized ");
//            System.out.println("a = " + object1.a);
//            System.out.println("b = " + object1.b);
//        }
//
//        catch(IOException ex)
//        {
//            System.out.println("IOException is caught");
//        }
//
//        catch(ClassNotFoundException ex)
//        {
//            System.out.println("ClassNotFoundException is caught");
//        }

    }
}