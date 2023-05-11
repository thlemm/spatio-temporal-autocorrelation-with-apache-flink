package de.thlemm.constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Locations {

    public static void main(String[] args) {
        List<Integer> locations = getLocations(10);
        System.out.println(locations);

        List<Integer> shuffledLocations = shuffleLocations(locations);
        System.out.println(shuffledLocations);
    }

    public static List<Integer> getLocations(int n) {
        List<Integer> locations = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            locations.add(i);
        }
        return locations;
    }

    public static List<Integer> shuffleLocations(List<Integer> locations) {
        Collections.shuffle(locations);
        return locations;
    }
}
