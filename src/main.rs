use inindexer::message_provider::ParallelProviderStreamer;
use inindexer::multiindexer::{MapError, ParallelJoinIndexers};
use inindexer::near_indexer_primitives::types::BlockHeight;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::near_utils::TESTNET_GENESIS_BLOCK_HEIGHT;
use inindexer::neardata::NeardataProvider;
use inindexer::neardata_old::OldNeardataProvider;
use inindexer::{
    run_indexer, AutoContinue, BlockIterator, IndexerOptions, MessageStreamer,
    PreprocessTransactionsSettings,
};
use near_jsonrpc_client::JsonRpcClient;
use redis::aio::ConnectionManager;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const MAX_BLOCKS_IN_REDIS: usize = 60 * 60 * 2; // up to 2 hours worth of data can be effortlessly recovered

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
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
            is_testnet: true,
        };
        let ft_indexer = ft_indexer::FtIndexer(
            ft_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let nft_indexer = nft_indexer::NftIndexer(
            nft_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let log_indexer = log_indexer::LogIndexer(
            log_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let new_token_indexer = new_token_indexer::NewTokenIndexer::new(
            new_token_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
            JsonRpcClient::connect(
                std::env::var("RPC_URL")
                    .unwrap_or("https://archival-rpc.testnet.near.org".to_string()),
            ),
            new_token_indexer::txt_file_storage::TxtFileStorage::new("testnet_known_tokens.txt")
                .await,
            new_token_indexer::txt_file_storage::TxtFileStorage::new(
                "testnet_known_nft_tokens.txt",
            )
            .await,
        );
        let block_indexer = block_indexer::BlockIndexer(
            block_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let tx_indexer = tx_indexer::TxIndexer(
            tx_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let mut indexer = trade_indexer
            .parallel_join(ft_indexer)
            .parallel_join(nft_indexer)
            .parallel_join(log_indexer)
            .parallel_join(block_indexer)
            .parallel_join(tx_indexer)
            .map_error(anyhow::Error::msg)
            .parallel_join(new_token_indexer);

        run_indexer(
            &mut indexer,
            NeardataProvider::testnet(),
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
            nft_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let ft_indexer = ft_indexer::FtIndexer(
            ft_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let potlock_indexer = potlock_indexer::PotlockIndexer(
            potlock_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let trade_indexer = trade_indexer::TradeIndexer {
            handler: trade_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
            is_testnet: false,
        };
        let new_token_indexer = new_token_indexer::NewTokenIndexer::new(
            new_token_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
            JsonRpcClient::connect(
                std::env::var("RPC_URL")
                    .unwrap_or("https://archival-rpc.mainnet.near.org".to_string()),
            ),
            new_token_indexer::txt_file_storage::TxtFileStorage::new("known_tokens.txt").await,
            new_token_indexer::txt_file_storage::TxtFileStorage::new("known_nft_tokens.txt").await,
        );
        let socialdb_indexer = socialdb_indexer::SocialDBIndexer(
            socialdb_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let log_indexer = log_indexer::LogIndexer(
            log_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let block_indexer = block_indexer::BlockIndexer(
            block_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let tx_indexer = tx_indexer::TxIndexer(
            tx_indexer::redis_handler::PushToRedisStream::new(
                connection.clone(),
                MAX_BLOCKS_IN_REDIS,
            )
            .await,
        );
        let mut indexer = nft_indexer
            .parallel_join(ft_indexer)
            .parallel_join(trade_indexer)
            .parallel_join(socialdb_indexer)
            .parallel_join(log_indexer)
            .parallel_join(block_indexer)
            .parallel_join(tx_indexer)
            .map_error(anyhow::Error::msg)
            .parallel_join(potlock_indexer)
            .parallel_join(new_token_indexer);

        let provider: EitherStreamer = if std::env::var("PARALLEL").is_ok() {
            EitherStreamer::Parallel(ParallelProviderStreamer::new(
                OldNeardataProvider::mainnet(),
                10,
            ))
        } else {
            EitherStreamer::Single(NeardataProvider::mainnet())
        };

        run_indexer(
            &mut indexer,
            provider,
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

enum EitherStreamer {
    Parallel(ParallelProviderStreamer<OldNeardataProvider>),
    Single(NeardataProvider),
}

#[async_trait::async_trait]
impl MessageStreamer for EitherStreamer {
    type Error = String;

    async fn stream(
        self,
        range: impl Iterator<Item = BlockHeight> + Send + 'static,
    ) -> Result<
        (
            JoinHandle<Result<(), Self::Error>>,
            mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        match self {
            EitherStreamer::Parallel(provider) => {
                let res = provider.stream(range).await;
                match res {
                    Ok((join_handle, receiver)) => Ok((
                        tokio::spawn(async move {
                            join_handle.await.unwrap().map_err(|e| format!("{e:?}"))
                        }),
                        receiver,
                    )),
                    Err(e) => Err(format!("{e:?}")),
                }
            }
            EitherStreamer::Single(provider) => {
                let res = provider.stream(range).await;
                match res {
                    Ok((join_handle, receiver)) => Ok((
                        tokio::spawn(async move {
                            join_handle.await.unwrap().map_err(|e| format!("{e:?}"))
                        }),
                        receiver,
                    )),
                    Err(e) => Err(format!("{e:?}")),
                }
            }
        }
    }
}
