# all-indexers

A simple crate that combines [nft-indexer](https://github.com/INTEARnear/nft-indexer), [potlock-indexer](https://github.com/INTEARnear/potlock-indexer), [trade-indexer](https://github.com/INTEARnear/trade-indexer), [contract-indexer](https://github.com/INTEARnear/contract-indexer), and all other indexers in this organization (except for token indexer) in one MultiIndexer to save on bandwith.

To enable faster parallel fetching (for backfilling), set `PARALLEL=1` environment variable.
