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
