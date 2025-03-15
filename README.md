## Manus generated project
query:

> I need to use python to implement a distributed realtime computation platform, which is able to handle 10000+ TPS with limited resource, and P99 latency should less than 100ms including access external databases. 
> It also need to provide HA, disaster recovering, failover, throttling etc, making it as reliable as possible. 
> 
> Base on the platform, implement an 3D risk analysis system to detect any potential collision. For example, all electric car will report their location every second, and run some collision detection algorithm with location reported by other car. So the latency should be very fast before the collison happened, and the throughput could be high since traffic could be terrible during rush hour. And the data is extremely skew, there are many cars in the city, but no car in some remote place.
