package main

// we also need to decide what sort of a microservice architecture will we mimic!
// requirements -> easily scalable, i can add as many of a type of microservice as i want without causing breaking changes
// this would make testing more robust, being able to test our distributed logger with a wide variety of load

// let's pick CDNs to model our microservice architecture
// microservice1 -> origin server, holds all the content! only communicates with cache server
// microservice2 -> cache server, holds caches of content from the origin server.
// microservice3 -> request router, routes requests to cache servers. communicates with only cache servers.

// tasks which need to be performed by the microservice are as follows ->
// 1. At random intervals, simulate a task (task will depend upon which microservice role is being played)
// 2. Log every single event which takes place, send logs over localhost to the actor responsible for distributed logging
// 3. produce heartbeats at regular intervals

func main() {

}
