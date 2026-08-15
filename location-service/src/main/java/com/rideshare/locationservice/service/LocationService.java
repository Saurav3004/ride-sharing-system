package com.rideshare.locationservice.service;

import com.rideshare.locationservice.dto.DriverLocationRequest;
import com.rideshare.locationservice.dto.NearByDriverResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class LocationService {

    private final RedisTemplate<String,String> redisTemplate;

    // Redis key for all driver location
    private static final String DRIVERS_GEO_KEY = "drivers:location";

    /**
     * update driver location in redis
     * called every 3 seconds by driver's phone
     * Maps to redis GEOADD command
     */

    public void updateDriverLocation(DriverLocationRequest driverLocationRequest){
        log.info("Updating location for driver: {}", driverLocationRequest.getDriverId());

        //Important: Longitude first and Latitude second
        Point driverPoint = new Point(driverLocationRequest.getLongitude(),driverLocationRequest.getLatitude());

        redisTemplate.opsForGeo().add(DRIVERS_GEO_KEY,driverPoint,driverLocationRequest.getDriverId());

        log.info("Location updated for driver: {}", driverLocationRequest.getDriverId());
    }

    /**
     * Find nearby drivers within given radius
     * Called by Matching service on ride request
     * Maps to Redis GEORADIUS command
     */

    public List<NearByDriverResponse> findNearByDrivers(double latitude, double longitude, double radiusInKm){
        log.info("Finding drivers near lat: {} long: {} within: {}km",latitude,longitude,radiusInKm);

        Circle searchArea = new Circle(new Point(longitude,latitude),new Distance(radiusInKm, Metrics.KILOMETERS));

        GeoResults<RedisGeoCommands.GeoLocation<String>> results = redisTemplate.opsForGeo().radius(DRIVERS_GEO_KEY,
                searchArea,
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                        .includeCoordinates()
                        .includeDistance()
                        .sortAscending()
                        .limit(10)
        );

        List<NearByDriverResponse> nearByDrivers = new ArrayList<>();

        if(results != null){
            results.getContent().forEach(result -> {
                RedisGeoCommands.GeoLocation<String> location = result.getContent();
                nearByDrivers.add(new NearByDriverResponse(
                        location.getName(),
                        location.getPoint().getY(),
                        location.getPoint().getX(),
                        result.getDistance().getValue()
                ));
            });
        }

        log.info("Found {} drivers nearby ", nearByDrivers.size());
        return nearByDrivers;
    }

    /**
     * Remove driver when they go offline
     * Maps to redis ZREM command
     */

    public void removeDriver(String driverID){
        log.info("Removing driver: {}",driverID);
        redisTemplate.opsForGeo().remove(DRIVERS_GEO_KEY,driverID);
    }
}
