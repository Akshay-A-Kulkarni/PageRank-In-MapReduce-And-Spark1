package pr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyntheticGraph{

    private int K;
    private ArrayList<Vertex> vertices;

    public SyntheticGraph (int K){
        this.K = K;
        this.vertices = new ArrayList<Vertex>();
    }

    private void vertexSetGenerator(){
        float k_sq = this.K * this.K;
        for (Integer i = 1; i <= k_sq ; i++) {
            if( i % this.K != 0 ){
                Vertex v = new Vertex(i);
                v.addToAdjacencyList(i+1);
                v.setPR(1.0/k_sq);
                this.vertices.add(v);
            }
            else {
                Vertex v = new Vertex(i);
                v.addToAdjacencyList(0);
                v.setPR(1.0/k_sq);
                this.vertices.add(v);
            }
        }
    }
}