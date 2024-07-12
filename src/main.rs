use contract_indexer::txt_file_storage::TxtFileStorage;
use contract_indexer::RPC_URL;
use inindexer::multiindexer::{ChainIndexers, MapError};
use inindexer::neardata_server::NeardataServerProvider;
use inindexer::{
    run_indexer, AutoContinue, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use near_jsonrpc_client::JsonRpcClient;
use redis::aio::ConnectionManager;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let nft_indexer = nft_indexer::NftIndexer(
        nft_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000).await,
    );
    let potlock_indexer = potlock_indexer::PotlockIndexer(
        potlock_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000).await,
    );
    let trade_indexer = trade_indexer::TradeIndexer(
        trade_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000).await,
    );
    let contract_indexer = contract_indexer::ContractIndexer::new(
        contract_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000).await,
        JsonRpcClient::connect(std::env::var("RPC_URL").unwrap_or(RPC_URL.to_string())),
        TxtFileStorage::new("known_tokens.txt").await,
    );
    let mut indexer = nft_indexer
        .map_error(anyhow::Error::msg)
        .chain(potlock_indexer)
        .chain(trade_indexer.map_error(anyhow::Error::msg))
        .chain(contract_indexer);

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: if std::env::args().len() > 1 {
                // For debugging
                let msg = "Usage: `all-indexers` or `all-indexers [start-block] [end-block]`";
                BlockIterator::iterator(
                    std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg)
                        ..=std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                )
            } else {
                BlockIterator::AutoContinue(AutoContinue::default())
            },
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
                postfetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Indexer run failed");
}
