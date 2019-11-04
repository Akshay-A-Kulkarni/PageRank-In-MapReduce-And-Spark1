package pr;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;

public class Vertex implements Writable {
    private Integer  Vertex_ID;
    private Double Vertex_PR;
    private ArrayList<Integer> adjVertices;
    private int adjSize;
    boolean fullVertexFlag;

    public Vertex ()  {
        this.Vertex_ID = null;
        this.Vertex_PR = 1.0; // init value
        this.adjVertices = new ArrayList<Integer>();
        this.adjSize = this.adjVertices.size();
        this.fullVertexFlag = true;
    }
    @Override
    public void readFields(DataInput in) throws IOException{
        this.Vertex_ID  = in.readInt();
        this.Vertex_PR = in.readDouble();
        this.fullVertexFlag = in.readBoolean();
        if (this.fullVertexFlag) {
            this.adjSize = in.readInt();
            ArrayList<Integer> temp = new ArrayList<>();
            for (int i = 0; i < this.adjSize; i++) {
                temp.add(in.readInt());
            }
            this.adjVertices = temp;
        }
    }
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(this.Vertex_ID);
        out.writeDouble(this.Vertex_PR);
        out.writeBoolean(this.fullVertexFlag);
        if (this.fullVertexFlag) {
            out.writeInt(this.adjSize);
            for (int i = 0; i < this.adjSize; i++) {
                out.writeInt(this.adjVertices.get(i));
            }
        }
    }
    public void setVertex(int input){
        this.Vertex_ID = input;
    }
    public Integer getVertex(){
        return this.Vertex_ID;
    }
    public void setPR(Double input){
        this.Vertex_PR = input;
    }
    public void appendToAdjacencyList(Integer A){
        this.adjVertices.add(A);
        this.adjSize = this.adjVertices.size();
    }
    public void setflag(boolean i){
        this.fullVertexFlag = i;
    }
    public Double getPR(){
        return this.Vertex_PR;
    }
    public boolean getflag(){
        return this.fullVertexFlag;
    }
    public ArrayList<Integer> getAdjacencyList(){
        return this.adjVertices;
    }
    public Integer getAdjacencySize(){
        return this.adjVertices.size();
    }
    public String serializeToString(){
        return "#"+this.Vertex_ID.toString()+"@"+ this.Vertex_PR.toString()+"@"+this.adjVertices.toString()+"#";
    }
    public void deserializeFromString(String in){
        String value = in.trim();
        final String[] values  = value.replaceAll("[#]", "").split("@");
        final String[] adj = values[2].replaceAll("[\\[\\]]","").split(",");
        final ArrayList<Integer> adj_i= new ArrayList<>();
        for (String s:adj) {
            adj_i.add(Integer.parseInt(s));
        }

        this.Vertex_ID = Integer.parseInt(values[0]);
        this.Vertex_PR = Double.parseDouble(values[1]);
        this.adjVertices = adj_i;
        this.adjSize = adj_i.size();
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

}