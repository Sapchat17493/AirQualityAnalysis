package cs455.hadoop.helpers;

public class StateTemp implements Comparable<StateTemp> {
    private String state;
    private double avgTemp;

    public StateTemp(String state, double avgTemp) {
        this.avgTemp = avgTemp;
        this.state = state;
    }

    public double getAvgTemp() {
        return avgTemp;
    }

    public String getState() {
        return state;
    }


    @Override
    public int compareTo(StateTemp st) {
        if (this.avgTemp < st.avgTemp) {
            return 1;
        } else if (this.avgTemp > st.avgTemp) {
            return -1;
        } else {
            return 0;
        }
    }
}
