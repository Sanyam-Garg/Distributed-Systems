## Worker 
- [ ] Worker requests the coordinator for more tasks in a loop.
- [ ] Produce intermediate files after each map task. Basically, for each key-value pair, decide via hash in which reduce task number it should be processed, and then write to `mr-mapX-reduceY`
- [ ] Once all files have been processed, perform `nReduce` reduce tasks.

## Coordinator
- [ ] After submitting task to a worker, wait for max 10s for it to be completed. Else assign it to other worker.
- [ ] Maintain state for count of map tasks and reduce tasks.
- [ ] Once all map and reduce tasks have been completed, assign each worker a task to exit and then return `true` in `Done()` method.