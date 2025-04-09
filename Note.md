# Some changes for debugging:
- in src/clients/asynclient.go I currently have commented out that the coordination reqs go to every 3rd predecessor, so that on 3 shards, we can eliminate the overhead of the message sending.
