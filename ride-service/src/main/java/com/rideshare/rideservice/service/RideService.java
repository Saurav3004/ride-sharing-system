package com.rideshare.rideservice.service;

import com.rideshare.rideservice.dto.RideRequest;
import com.rideshare.rideservice.dto.RideResponse;
import com.rideshare.rideservice.event.RideRequestedEvent;
import com.rideshare.rideservice.model.Ride;
import com.rideshare.rideservice.model.RideStatus;
import com.rideshare.rideservice.repository.RideRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RideService {

    private final RideRepository rideRepository;
    private final KafkaTemplate<String, RideRequestedEvent> kafkaTemplate;

    private static final String RIDE_REQUESTED_TOPIC = "ride.requested";

    /**
     * create ride in DB with REQUESTED status
     */

    public RideResponse requestRide(RideRequest rideRequest){
        log.info("New ride request from rider: {}",rideRequest.getRiderId());

        //Step:1 - save ride to DB
        Ride ride = new Ride();
        ride.setRiderId(rideRequest.getRiderId());
        ride.setPickupLatitude(rideRequest.getPickupLatitude());
        ride.setPickupLongitude(rideRequest.getPickupLongitude());
        ride.setPickupAddress(rideRequest.getPickupAddress());
        ride.setDropLatitude(rideRequest.getDropLatitude());
        ride.setDropLongitude(rideRequest.getDropLongitude());
        ride.setDropAddress(rideRequest.getDropAddress());
        ride.setStatus(RideStatus.REQUESTED);
        ride.setEstimatedFare(calculateEstimateFare(rideRequest));

        Ride savedRide = rideRepository.save(ride);

        //Step:2 - publish event to kafka
        //Matching service will consume this and find nearest driver

        RideRequestedEvent event = new RideRequestedEvent(
                savedRide.getId(),
                savedRide.getRiderId(),
                savedRide.getPickupLatitude(),
                savedRide.getPickupLongitude(),
                savedRide.getPickupAddress(),
                savedRide.getDropLatitude(),
                savedRide.getDropLongitude(),
                savedRide.getDropAddress()
        );

        kafkaTemplate.send(RIDE_REQUESTED_TOPIC,savedRide.getId(),event);
        log.info("RideRequestedEvent published to kafka for ride: {}",savedRide.getId());

        savedRide.setStatus(RideStatus.MATCHING);
        rideRepository.save(savedRide);

        return mapToResponse(savedRide);
    }

    public void updateRideWithDriver(String rideId,String driverId){
        Ride ride = rideRepository.findById(rideId).orElseThrow(() -> new RuntimeException("Ride not found"));

        ride.setDriverId(driverId);
        ride.setStatus(RideStatus.ACCEPTED);
        rideRepository.save(ride);
    }

    public RideResponse startRide(String rideId){
        Ride ride = rideRepository.findById(rideId).orElseThrow(() -> new RuntimeException("Ride not found"));

        if(ride.getStatus() != RideStatus.ACCEPTED){
            throw new RuntimeException("Ride cannot started. current status is: " + ride.getStatus());
        }

        ride.setStatus(RideStatus.RIDE_STARTED);
        ride.setStartedAt(LocalDateTime.now());
        rideRepository.save(ride);

        return mapToResponse(ride);
    }

    public RideResponse completeRide(String rideId){
        Ride ride = rideRepository.findById(rideId).orElseThrow(() -> new RuntimeException("Ride not found"));

        if(ride.getStatus() != RideStatus.RIDE_STARTED){
            throw new RuntimeException("Ride cannot be complete. current status is: " + ride.getStatus());
        }

        ride.setStatus(RideStatus.COMPLETED);
        ride.setCompletedAt(LocalDateTime.now());
        ride.setActualFare(ride.getEstimatedFare());
        rideRepository.save(ride);

        return mapToResponse(ride);
    }

    public RideResponse cancelRide(String rideId){
        Ride ride = rideRepository.findById(rideId).orElseThrow(() -> new RuntimeException("Ride not found"));

        ride.setStatus(RideStatus.CANCELLED);
        rideRepository.save(ride);
        return mapToResponse(ride);
    }

    public RideResponse getRideById(String rideId){
        Ride ride = rideRepository.findById(rideId).orElseThrow(() -> new RuntimeException("Ride not found"));

        return mapToResponse(ride);
    }

    public List<RideResponse> getRidesByRider(String riderId){
        List<Ride> ride = rideRepository.findByRiderIdOrderByCreatedAtDesc(riderId);

        return ride.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    private double calculateEstimateFare(RideRequest rideRequest){
        // Simplified Haversine distance calculation

        double lat1 = Math.toRadians(rideRequest.getPickupLatitude());
        double long1 = Math.toRadians(rideRequest.getPickupLongitude());
        double lat2 = Math.toRadians(rideRequest.getDropLatitude());
        double long2 = Math.toRadians(rideRequest.getDropLongitude());

        double dLat = lat1 - lat2;
        double dLong = long1 - long2;

        double a = Math.pow(Math.sin(dLat/2),2)
                   +Math.cos(lat1) * Math.cos(lat2)
                   *Math.pow(Math.sin(dLong/2),2);

        double c = 2 * Math.asin(Math.sqrt(a));
        double distanceInKm = 6371 * c;

        //Base fare: 50Rs + 12Rs per km
        double fare = 50 + (distanceInKm * 12);
        return Math.round(fare * 100.0) / 100.0;
    }

    public RideResponse mapToResponse(Ride savedRide){
        RideResponse rideResponse = new RideResponse();

        rideResponse.setId(savedRide.getId());
        rideResponse.setRiderId(savedRide.getRiderId());
        rideResponse.setDriverId(savedRide.getDriverId());
        rideResponse.setPickupLatitude(savedRide.getPickupLatitude());
        rideResponse.setPickupLongitude(savedRide.getPickupLongitude());
        rideResponse.setPickupAddress(savedRide.getPickupAddress());
        rideResponse.setDropLatitude(savedRide.getDropLatitude());
        rideResponse.setDropLongitude(savedRide.getDropLongitude());
        rideResponse.setDropAddress(savedRide.getDropAddress());
        rideResponse.setRideStatus(savedRide.getStatus());
        rideResponse.setActualFare(savedRide.getActualFare());
        rideResponse.setEstimateFare(savedRide.getEstimatedFare());
        rideResponse.setCreatedAt(savedRide.getCreatedAt());
        rideResponse.setStartedAt(savedRide.getStartedAt());
        rideResponse.setCompletedAt(savedRide.getCompletedAt());

        return rideResponse;
    }
}
