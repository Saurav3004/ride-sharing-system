package com.rideshare.matchingservice.service;

import com.rideshare.matchingservice.client.LocationServiceClient;
import com.rideshare.matchingservice.dto.NearByDriverResponse;
import com.rideshare.matchingservice.event.RideMatchedEvent;
import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class MatchingService {

    private final LocationServiceClient locationServiceClient;
    private final KafkaTemplate<String, RideMatchedEvent> kafkaTemplate;

    private static final String RIDE_MATCHED_TOPIC = "ride.matched";
    private static final double DEFAULT_SEARCH_RADIUS_KM = 5.0;

    /**
     * Main matching algorithm
     * Called when rideRequestedEvent is consumed from kafka
     * @param rideRequestedEvent
     *
     * STEPS:
     * 1. Ask Location service for nearByDriver
     * 2. Score each driver and pick the best one
     */
    public void matchDriverForRide(RideRequestedEvent rideRequestedEvent){
        List<NearByDriverResponse> nearByDrivers =
                locationServiceClient.getNearByDrivers(rideRequestedEvent.getPickupLatitude(),
                        rideRequestedEvent.getPickupLongitude(),DEFAULT_SEARCH_RADIUS_KM);

        if(nearByDrivers.isEmpty()){
            log.warn("No drivers found near ride");
            return;
        }

        // STEP 2: Score each driver and pick the best one
        Optional<NearByDriverResponse> bestDriver = findBestDriver(nearByDrivers);
        if(bestDriver.isEmpty()){
            log.warn("could not find suitable driver for ride");
            return;
        }

        NearByDriverResponse assignedDriver = bestDriver.get();

        // STEP 3: Published rideMatchedEvent to kafka
        RideMatchedEvent rideMatchedEvent = new RideMatchedEvent();
        rideMatchedEvent.setDriverId(assignedDriver.getDriverId());
        rideMatchedEvent.setRiderId(rideRequestedEvent.getRiderId());
        rideMatchedEvent.setRideId(rideRequestedEvent.getRideId());
        rideMatchedEvent.setDriverLatitude(assignedDriver.getLatitude());
        rideMatchedEvent.setDriverLongitude(assignedDriver.getLongitude());
        rideMatchedEvent.setDistanceToPickupKm(assignedDriver.getDistanceInKm());

        kafkaTemplate.send(RIDE_MATCHED_TOPIC,rideRequestedEvent.getRideId(),rideMatchedEvent);
        log.info("RideMatchedEvent published");
    }

    /**
     * Driver scoring algorithms
     *
     * Distance: 70%
     * Rating: 30%
     *
     * Score = (1 / Distance) * distanceWeight + rating * ratingWeight
     * @param drivers
     * @return
     */

    private Optional<NearByDriverResponse> findBestDriver(List<NearByDriverResponse> drivers){

        double distanceWeight = 0.7;
        double ratingWeight = 0.3;

        return drivers.stream().max(Comparator.comparingDouble(driver -> {
            //Distance score: closer = higher score
            //Add 0.1 to avoid divide by zero
            double distanceScore = 1.0/(driver.getDistanceInKm() + 0.1);

            //Simulated rating between 4.0 and 5.0
            // In production: Fetch from Driver service

            double rating = 4.0 + Math.random();

            return (distanceScore * distanceWeight) + (rating * ratingWeight);

        }));

    }
}
