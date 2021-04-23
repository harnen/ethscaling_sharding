# Object Migration Oracle

## Problem
The incoming Ethereum 2.0 sharding promises to improve the performance of the system. Transactions can be processed in parallel by independent groups of miners place in different shards. Thus, more shards = higher TPS.
Shards can efficiently process transactions that modify their internal state (i.e., accounts placed in the given shard). This requires a simple intra-shard consensus. 
However, transactions modifying objects placed across multiple shards are more complicated, require consensus in all the involved shard + some coordination. 
The problem becomes more severe when many accounts are involved in a single cross-shard transactions. If not done correctly, cross-shard transactions can significantly lower the TPS of the entire chain. 

## Solution
In this project we plan to investigate future cross-shard interactions. To do this we:
* **Extract data from the current blockchain** - we plan to extract internal and external transactions and get all the accounts involved in each transaction. "Involved" means, who's state was modified during the transaction. I.e., in [this contract](https://gist.github.com/harnen/05f2fa58a3ffeb663acc5bd21d9b7182), when invoking payAll(), all the accounts from `users` will be affected. 
* **Analyze relations between objects** - we want to see if we can identify groups of frequently communicating nodes that should be place in the same shards or some other interaction patterns. 
* **Provide an account migration mechanism** - we plan to develop a policy that'll be able to automatically distribute and migrate objects across shards in an optimal way to maximize the global TPS.
