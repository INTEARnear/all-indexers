use inindexer::multiindexer::{ChainIndexers, MapError};
use inindexer::near_utils::TESTNET_GENESIS_BLOCK_HEIGHT;
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

    if std::env::var("TESTNET").is_ok() {
        log::warn!("Running all-indexers on testnet");

        let trade_indexer = trade_indexer::TradeIndexer {
            handler: trade_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                10_000,
                true,
            )
            .await,
            is_testnet: true,
        };
        let log_indexer = log_indexer::LogIndexer(
            log_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 100_000, true)
                .await,
        );
        let new_token_indexer = new_token_indexer::NewTokenIndexer::new(
            new_token_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                10_000,
                true,
            )
            .await,
            JsonRpcClient::connect(
                std::env::var("RPC_URL")
                    .unwrap_or("https://archival-rpc.testnet.near.org".to_string()),
            ),
            new_token_indexer::txt_file_storage::TxtFileStorage::new("testnet_known_tokens.txt")
                .await,
        );
        let tps_indexer = tps_indexer::TpsIndexer(
            tps_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                10_000_000,
                true,
            )
            .await,
        );
        let tx_indexer = tx_indexer::TxIndexer(
            tx_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 1_000_000, true)
                .await,
        );
        let mut indexer = trade_indexer
            .map_error(anyhow::Error::msg)
            .chain(log_indexer.map_error(anyhow::Error::msg))
            .chain(new_token_indexer)
            .chain(tps_indexer.map_error(anyhow::Error::msg))
            .chain(tx_indexer.map_error(anyhow::Error::msg));

        run_indexer(
            &mut indexer,
            NeardataServerProvider::testnet(),
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
                    BlockIterator::AutoContinue(AutoContinue {
                        save_location: Box::new("last-processed-block-testnet.txt"),
                        start_height_if_does_not_exist: TESTNET_GENESIS_BLOCK_HEIGHT,
                        ..Default::default()
                    })
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
    } else {
        let nft_indexer = nft_indexer::NftIndexer(
            nft_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000).await,
        );
        let ft_indexer = ft_indexer::FtIndexer(
            ft_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 100_000).await,
        );
        let potlock_indexer = potlock_indexer::PotlockIndexer(
            potlock_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000)
                .await,
        );
        let trade_indexer = trade_indexer::TradeIndexer {
            handler: trade_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                10_000,
                false,
            )
            .await,
            is_testnet: false,
        };
        let new_token_indexer = new_token_indexer::NewTokenIndexer::new(
            new_token_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                10_000,
                false,
            )
            .await,
            JsonRpcClient::connect(
                std::env::var("RPC_URL")
                    .unwrap_or("https://archival-rpc.mainnet.near.org".to_string()),
            ),
            new_token_indexer::txt_file_storage::TxtFileStorage::new("known_tokens.txt").await,
        );
        let socialdb_indexer = socialdb_indexer::SocialDBIndexer(
            socialdb_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 10_000)
                .await,
        );
        let log_indexer = log_indexer::LogIndexer(
            log_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 100_000, false)
                .await,
        );
        let tps_indexer = tps_indexer::TpsIndexer(
            tps_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                10_000_000,
                false,
            )
            .await,
        );
        let tx_indexer = tx_indexer::TxIndexer(
            tx_indexer::redis_handler::PushToRedisStream::new(connection.clone(), 1_000_000, false)
                .await,
        );
        let mut indexer = nft_indexer
            .map_error(anyhow::Error::msg)
            .chain(ft_indexer.map_error(anyhow::Error::msg))
            .chain(potlock_indexer)
            .chain(trade_indexer.map_error(anyhow::Error::msg))
            .chain(new_token_indexer)
            .chain(socialdb_indexer.map_error(anyhow::Error::msg))
            .chain(log_indexer.map_error(anyhow::Error::msg))
            .chain(tps_indexer.map_error(anyhow::Error::msg))
            .chain(tx_indexer.map_error(anyhow::Error::msg));

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
}
