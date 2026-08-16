package com.rideshare.rideservice.dto;

import com.rideshare.rideservice.model.RideStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RideResponse {

    private String id;

    private String riderId;

    private String driverId;

    private double pickupLatitude;

    private double pickupLongitude;

    private String pickupAddress;

    private double dropLatitude;

    private double dropLongitude;

    private String dropAddress;

    private RideStatus rideStatus;

    private double actualFare;

    private double estimateFare;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
}
