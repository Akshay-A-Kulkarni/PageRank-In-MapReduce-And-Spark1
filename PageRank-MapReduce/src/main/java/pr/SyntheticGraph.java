package pr;

import org.apache.directory.shared.kerberos.codec.apRep.actions.ApRepInit;

import java.io.*;
import java.util.ArrayList;


public class SyntheticGraph {

    private int K;
    private ArrayList<Vertex> vertices;

    public SyntheticGraph(int K) {
        this.K = K;
        this.vertices = new ArrayList<Vertex>();
    }


    private ArrayList<Vertex> vertexSetGenerator() {
        int k_sq = this.K * this.K;
        for (Integer i = 1; i <= k_sq; i++) {
            if (i % this.K != 0) {
                Vertex v1 = new Vertex();
                v1.setVertex(i);
                v1.setPR(1.0 / k_sq);
                v1.appendToAdjacencyList((i+1));
                this.vertices.add(v1);
            }
            else {
                Vertex v2 = new Vertex();
                v2.setVertex(i);
                v2.setPR(1.0 / k_sq);
                v2.appendToAdjacencyList(0);
                this.vertices.add(v2);

            }
        }
        return this.vertices;
    }

    public static void main(String[] args) throws IOException {

        ArrayList<Vertex> g = new SyntheticGraph(1000).vertexSetGenerator();
        PrintWriter writer = new PrintWriter("input/input.txt", "UTF-8");

        for(Vertex v : g){
            writer.println(v.serializeToString());
            }
        writer.close();

    }
}