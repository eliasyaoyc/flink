package training.ridecleansing;

import org.apache.flink.common.datatypes.TaxiRide;

import java.time.Instant;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class RideCleansingTestBase {
    public static TaxiRide testRide(float startLon, float startLat, float endLon, float endLat){
        return new TaxiRide(1L, true, Instant.EPOCH, startLon, startLat, endLon,endLat, (short)1,0,0);
    }
}
