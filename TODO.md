## Worker 
- [ ] Worker requests the coordinator for more tasks in a loop.
- [ ] Produce intermediate files after each map task. Basically, running a map task will give an array of key-value pairs. Split that array into `nReduce` parts, and write each part to file `mr-mapX-reduceY` where X is the map task num and Y is the reduce task num which will be performed.
- [ ] Once all files have been processed, perform `nReduce` reduce tasks.

## Coordinator
- [ ] After submitting task to a worker, wait for max 10s for it to be completed. Else assign it to other worker.
- [ ] Maintain state for count of map tasks and reduce tasks.
- [ ] Once all map and reduce tasks have been completed, assign each worker a task to exit and then return `true` in `Done()` method.