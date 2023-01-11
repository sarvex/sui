// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO(william) add metrics
// TODO(william) make configurable

use futures::stream::FuturesOrdered;

use sui_types::base_types::ObjectDigest;
use tracing::debug;
use tracing::log::warn;

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use fastcrypto::hash::{Digest, MultisetHash};
use mysten_metrics::spawn_monitored_task;
use sui_types::accumulator::Accumulator;
use sui_types::committee::EpochId;
use sui_types::error::{SuiError, SuiResult};
use sui_types::messages::TransactionEffects;
use sui_types::messages_checkpoint::{
    CheckpointSequenceNumber, CheckpointWatermark,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio::task::JoinHandle;
use typed_store::rocks::{DBMap, TypedStoreError};
use typed_store::traits::{TableSummary, TypedStoreDebug};
use typed_store::Map;
use typed_store_derive::DBMapUtils;

use crate::authority::AuthorityState;
use crate::checkpoints::CheckpointStore;

const ROOT_STATE_HASH_KEY: u64 = 0;
const MAX_CONCURRENCY: usize = 100;

type AccumulatorTaskBuffer = FuturesOrdered<JoinHandle<SuiResult<(Accumulator, State)>>>;
type EndOfEpochFlag = bool;

#[derive(Debug, Clone)]
pub struct State {
    effects: Vec<TransactionEffects>,
    end_of_epoch_flag: EndOfEpochFlag,
    checkpoint_seq_num: CheckpointSequenceNumber,
}

#[derive(DBMapUtils)]
pub struct StateAccumulatorStore {
    // TODO: implement pruning policy as most of these tables can
    // be cleaned up at end of epoch
    watermarks: DBMap<CheckpointWatermark, CheckpointSequenceNumber>,

    pub state_hash_by_checkpoint: DBMap<CheckpointSequenceNumber, Accumulator>,

    // A live / hot object representing the running root state hash, computed from
    // the running accumulation of checkpoint state hashes. As such, should NEVER be
    // relied upon for snapshot verification or consensus.
    //
    // TODO: MUST BE THREAD SAFE
    pub(self) root_state_hash: DBMap<u64, Accumulator>,

    // Finalized root state digest for epoch, to be included in CheckpointSummary
    // of last checkpoint of epoch. These values should only ever be written once
    // and never changed
    pub root_state_digest_by_epoch: DBMap<EpochId, Digest<32>>,
}

impl StateAccumulatorStore {
    pub fn new(path: &Path) -> Arc<Self> {
        Arc::new(Self::open_tables_read_write(path.to_path_buf(), None, None))
    }

    /// Returns future containing the state digest for the given epoch
    /// once available
    pub async fn notify_read_state_digest(&self, epoch: EpochId) -> SuiResult {
        todo!()
    }

    pub fn get_state_hash_for_checkpoint(
        &self,
        checkpoint: &CheckpointSequenceNumber,
    ) -> Result<Option<Accumulator>, TypedStoreError> {
        self.state_hash_by_checkpoint.get(checkpoint)
    }

    pub fn get_most_recent_root_state_hash(&self) -> Result<Option<Accumulator>, TypedStoreError> {
        self.root_state_hash.get(&ROOT_STATE_HASH_KEY)
    }

    /// Idempotent because checkpoints are final. Moreover, we will
    /// often have the same checkpoint forwarded from different
    /// components (CheckpointOutput/CheckpointExecutor) and do not
    /// want to double count against the root state hash.
    pub fn insert_state_hash_for_checkpoint(
        &self,
        state_hash: Accumulator,
        checkpoint: CheckpointSequenceNumber,
    ) -> Result<(), TypedStoreError> {
        if self.state_hash_by_checkpoint.contains_key(&checkpoint)? {
            Ok(())
        } else {
            self.state_hash_by_checkpoint
                .insert(&checkpoint, &state_hash)
        }
    }

    pub fn get_highest_hashed_checkpoint_seq_number(
        &self,
    ) -> Result<Option<CheckpointSequenceNumber>, TypedStoreError> {
        if let Some(highest_hashed) = self.watermarks.get(&CheckpointWatermark::HighestHashed)? {
            Ok(Some(highest_hashed))
        } else {
            Ok(None)
        }
    }

    pub fn get_highest_accumulated_checkpoint_seq_number(
        &self,
    ) -> Result<Option<CheckpointSequenceNumber>, TypedStoreError> {
        if let Some(highest_accumulated) = self
            .watermarks
            .get(&CheckpointWatermark::HighestAccumulated)?
        {
            Ok(Some(highest_accumulated))
        } else {
            Ok(None)
        }
    }

    pub fn get_highest_checkpoint_ready_to_hash_seq_number(
        &self,
    ) -> Result<Option<CheckpointSequenceNumber>, TypedStoreError> {
        if let Some(highest_ready) = self
            .watermarks
            .get(&CheckpointWatermark::HighestReadyToHash)?
        {
            Ok(Some(highest_ready))
        } else {
            Ok(None)
        }
    }

    pub fn update_highest_hashed_checkpoint_seq_number(
        &self,
        checkpoint_seq_num: &CheckpointSequenceNumber,
    ) -> Result<(), TypedStoreError> {
        self.watermarks
            .insert(&CheckpointWatermark::HighestHashed, checkpoint_seq_num)
    }

    pub fn update_highest_accumulated_checkpoint_seq_number(
        &self,
        checkpoint_seq_num: &CheckpointSequenceNumber,
    ) -> Result<(), TypedStoreError> {
        self.watermarks
            .insert(&CheckpointWatermark::HighestAccumulated, checkpoint_seq_num)
    }

    pub fn update_highest_checkpoint_ready_to_hash_seq_number(
        &self,
        checkpoint_seq_num: &CheckpointSequenceNumber,
    ) -> Result<(), TypedStoreError> {
        self.watermarks
            .insert(&CheckpointWatermark::HighestReadyToHash, checkpoint_seq_num)
    }
}

/// Thread safe clone-able handle for enqueueing items to Accumulator
#[derive(Clone)]
pub struct EnqueueHandle {
    sender: mpsc::Sender<State>,
    store: Arc<StateAccumulatorStore>,
}

impl EnqueueHandle {
    /// Enqueue checkpoint state for accumulation. A checkpoint shall
    /// be processed (accumulated both individually and towards
    /// the root state hash) exactly once
    ///
    /// NOTE: When called from executor, this should be called before we increment
    /// highest executed checkpoint watermark, otherwise we may inefficiently read effects
    /// from the effects store rather than using effects passed in memory throuhg enqueue.
    /// This may happen due to a race between scheduling new checkpoints for hashing and
    /// checking the queue.
    pub async fn enqueue(&self, state: State) -> SuiResult {
        let seq_num = state.checkpoint_seq_num;

        let highest = self.store.get_highest_checkpoint_ready_to_hash_seq_number().unwrap();

        match highest {
            // Check to ensure items are enqueued in order and no checkpoint sequence
            // numbers are skipped
            Some(highest) => {
                match seq_num - highest {
                    // next checkpoint in sequence - happy path
                    1 => {},
                    
                    u64::MIN..=0 => {
                        warn!("Checkpoint effects for sequence number {seq_num:?} already enqueued for accumulation");
                        return Ok(());
                    },
                    
                    2..=u64::MAX => panic!("Checkpoint seq num {seq_num:?} contents enqueued for accumulation out of order"),
                }
            }
            None => {
                if seq_num != 0 {
                    panic!("Checkpoint seq num {seq_num:?} contents enqueued for accumulation out of order");
                }
            }
        }

        // note that we increment the watermark irrespective of whether `send` succeeds
        // or not. This ensures that it will eventually be handled even in a crash
        // recovery scenario
        self.store.update_highest_checkpoint_ready_to_hash_seq_number(&seq_num)
            .expect("StateAccumulator: failed to read highest cehckpoint ready to hash");

        if state.end_of_epoch_flag {
            // In this case we want to block until we have capacity to guarantee that this
            // will eventually be accumulated
            self.sender.send(state).await.map_err(|err| {
                SuiError::from(
                    format!("Blocking enqueue on StateAccumulator failed: {err:?}").as_str(),
                )
            })?;
        } else {
            // In this case we can do best effort. If buffer is full, we will eventually
            // accumulate the checkpoint contents
            self.sender.try_send(state).unwrap_or_else(|e| {
                warn!("StateAccumulator: best effort enqueue failed: {e:?}");
            })
        }

        Ok(())
    }
}

pub struct StateAccumulator {
    store: Arc<StateAccumulatorStore>,
    authority_state: Arc<AuthorityState>,
    checkpoint_store: Arc<CheckpointStore>,
    queue: mpsc::Receiver<State>,
    sender_handle: EnqueueHandle,
    /// Saves the latest State sent through the queue so
    /// that we don't have to read from db once in follow mode
    latest_enqueued_state: Option<State>,
    /// Keeps track of the highest sequence number that we have 
    /// enqueued for accumulation since instantiation, thereby 
    /// preventing us from rescheduling something currently pending
    highest_scheduled_seq_num: Option<CheckpointSequenceNumber>,
}

impl StateAccumulator {
    pub fn new(
        authority_state: Arc<AuthorityState>,
        checkpoint_store: Arc<CheckpointStore>,
        queue_size: u64,
        store_path: &Path,
    ) -> (Self, EnqueueHandle) {
        let store = StateAccumulatorStore::new(store_path);
        let (sender, receiver) = mpsc::channel(queue_size as usize);
        let handle = EnqueueHandle {
            sender,
            store: store.clone(),
        };

        (
            Self {
                store,
                authority_state,
                checkpoint_store,
                queue: receiver,
                // Only saved for self enqueueing during crash recovery
                sender_handle: handle.clone(),
                latest_enqueued_state: None,
                highest_scheduled_seq_num: None,
            },
            handle,
        )
    }

    pub async fn run(&mut self, epoch: EpochId) {
        self.handle_crash_recovery()
            .await
            .expect("StateAccumulator crash recovery cannot fail");

        self.gen_checkpoint_state_hashes().await;
        self.accumulate(epoch).await.expect("Accumulation failed");
    }

    /// To ensure consistency in the case of a crash after a checkpoint is enqueued
    /// for hashing but before the checkpoint hash is committed, this function re-enqueues
    /// all checkpoints between StateAccumulator::HighestHashed and
    /// CheckpointExecutor::HighestExecuted. By doing this, we provide an eventual
    /// consistency guarantee to components after the call to enqueue. Note that this
    /// also works for checkpoint creation without any extra work, as checkpoints created
    /// locally will still be considered by CheckpointExecutor. Lastly, for the special
    /// case of creation of last checkpoint of the epoch, this will not be needed, as checkpoint
    /// creation will block on enqueue, and then block waiting for the root state hash, before
    /// moving on with consensus. If we fail during wait, CheckpointBuilder will try again.
    ///
    /// IMPORTANT: must be called before any external tasks attempt to enqueue, otherwise
    /// may result in errors from enqueueing checkpoints out of order
    async fn handle_crash_recovery(&self) -> SuiResult {
        let next_to_hash = self
            .store
            .get_highest_hashed_checkpoint_seq_number()?
            .map(|num| num.saturating_add(1))
            .unwrap_or(0);
        let last_to_hash = self
            .checkpoint_store
            .get_highest_executed_checkpoint_seq_number()?
            .map(|num| num.saturating_add(1))
            .unwrap_or(0);

        for seq_num in next_to_hash..last_to_hash {
            self.sender_handle
                .enqueue(self.get_state_for_checkpoint_seq_num(seq_num))
                .await
                .map_err(|err| {
                    SuiError::from(
                        format!("StateAccumulator enqueue error: {err:?}").as_str(),
                    )
                })?;
        }
        Ok(())
    }

    fn get_state_for_checkpoint_seq_num(
        &self,
        checkpoint_seq_num: CheckpointSequenceNumber,
    ) -> State {
        let checkpoint = self
            .checkpoint_store
            .get_checkpoint_by_sequence_number(checkpoint_seq_num)
            .unwrap_or_else(|_| panic!("StateAccumulator: failed to get checkpoint {checkpoint_seq_num:?} from store"))
            .unwrap_or_else(|| panic!("StateAccumulator: expected checkpoint {checkpoint_seq_num:?} to exist in store"));

        let contents = self
            .checkpoint_store
            .get_checkpoint_contents(&checkpoint.content_digest())
            .unwrap_or_else(|_| panic!("Failed to get checkpoint contents for checkpoint {checkpoint_seq_num:?}"))
            .unwrap_or_else(|| panic!("Expected checkpoint {checkpoint_seq_num:?} contents to exist"));

        let effects: Vec<TransactionEffects> = contents
            .iter()
            .map(|ex_digest| {
                let tx_digest = ex_digest.transaction;
                self.authority_state
                    .database
                    .get_effects(&tx_digest)
                    .unwrap_or_else(|_| panic!("StateAccumulator: Failed to get effects for tx digest {tx_digest:?}"))
            })
            .collect();

        State {
            effects,
            end_of_epoch_flag: checkpoint.next_epoch_committee().is_some(),
            checkpoint_seq_num: checkpoint.sequence_number(),
        }
    }

    /// Reads checkpoints from the queue and manages parallel tasks (one
    /// per checkpoint) to read the associated transaction effects and
    /// gen and save the checkpoint state hash. Updates HighestHashed
    /// watermark. Returns when we have hashed all checkpoints for the 
    /// current epoch.
    async fn gen_checkpoint_state_hashes(&mut self) {
        let mut pending: AccumulatorTaskBuffer = FuturesOrdered::new();

        loop {
            self.schedule_accumulator_tasks(&mut pending)
                .unwrap_or_else(|err| {
                    panic!("Failed to schedule accumulator tasks: {err:?}")
                });

            tokio::select! {
                Some(Ok(Ok((acc, State { checkpoint_seq_num, end_of_epoch_flag, .. })))) = pending.next() => {
                    if let Some(prev_highest) = self.store.get_highest_hashed_checkpoint_seq_number().unwrap() {
                        assert_eq!(checkpoint_seq_num, prev_highest + 1);
                    } else {
                        assert_eq!(checkpoint_seq_num, 0);
                    }

                    debug!(
                        "Bumping highest_hashed_checkpoint watermark to {:?}",
                        checkpoint_seq_num,
                    );
                    // TODO bump a corresponding metric

                    // write checkpoint hash and update watermark in a batch in the main task
                    // to ensure watermark is updated sequentially and the two writes are atomic
                    let batch = self.store.state_hash_by_checkpoint.batch();

                    let batch = batch.insert_batch(
                        &self.store.state_hash_by_checkpoint,
                        std::iter::once((&checkpoint_seq_num, acc)),
                    ).expect("StateAccumulator: failed to insert state hash batch");
                    let batch = batch.insert_batch(
                        &self.store.watermarks,
                        std::iter::once((&CheckpointWatermark::HighestHashed, checkpoint_seq_num)),
                    ).expect("StateAccumulator: failed to insert highest hashed watermark batch");
            
                    batch.write().expect("StateAccumulator: failed to write hashed checkpoint batch");
                    
                    if end_of_epoch_flag {
                        return;
                    }
                }
                next = self.queue.recv() => match next {
                    Some(state) => {
                        debug!(
                            ?state,
                            "StateAccumulator: received enqueued state",
                        );
                        self.latest_enqueued_state = Some(state);
                    }
                    None => panic!("StateAccumulator: all senders have been dropped"),
                }
            }
        }
    }

    fn schedule_accumulator_tasks(&mut self, pending: &mut AccumulatorTaskBuffer) -> SuiResult {
        let latest_enqueued_state = match self.latest_enqueued_state.clone() {
            Some(state) => state,
            // Either nothing to hash or we have to catch up
            None => {
                if let Some(seq_num) = self
                    .store
                    .get_highest_checkpoint_ready_to_hash_seq_number()
                    .unwrap_or_else(|err| {
                        panic!("Failed to read highest checkpoint ready to be hashed: {:?}", err)
                    })
                {
                    let state = self.get_state_for_checkpoint_seq_num(seq_num);
                    self.latest_enqueued_state = Some(state.clone());
                    state
                } else {
                    return Ok(());
                }
            }
        };

        let highest_hashed_seq_num = self
            .store
            .get_highest_hashed_checkpoint_seq_number()
            .unwrap_or_else(|err| {
                panic!(
                    "StateAccumulator: failed to read highest hashed checkpoint sequence number: {err:?}",
                )
            });

        let next_to_hash = std::cmp::max(
            highest_hashed_seq_num
                .map(|highest| highest.saturating_add(1))
                .unwrap_or(0),
            self.highest_scheduled_seq_num
                .map(|highest| highest.saturating_add(1))
                .unwrap_or(0),
        );

        match next_to_hash.cmp(&latest_enqueued_state.checkpoint_seq_num) {
            // fully caught up case
            Ordering::Greater => return Ok(()),
            // follow case. Avoid reading from DB
            Ordering::Equal => {
                if pending.len() < MAX_CONCURRENCY {
                    return self.schedule_task(latest_enqueued_state, pending);
                }
            }
            // Need to catch up more than 1. Read from store
            Ordering::Less => {
                for i in next_to_hash..=latest_enqueued_state.checkpoint_seq_num {
                    if pending.len() >= MAX_CONCURRENCY {
                        break;
                    }
                    let state = self.get_state_for_checkpoint_seq_num(i);
                    self.schedule_task(state, pending)?;
                }
            }
        }

        Ok(())
    }

    fn schedule_task(&mut self, state: State, pending: &mut AccumulatorTaskBuffer) -> SuiResult {
        let store = self.store.clone();
        let state_clone = state.clone();

        pending.push_back(spawn_monitored_task!(async move {
            let accumulator = hash_checkpoint_effects(state_clone.clone(), store)?;
            Ok((accumulator, state_clone))
        }));

        self.highest_scheduled_seq_num = Some(state.checkpoint_seq_num);
        Ok(())
    }

    /// Ideal: Using differences between highest_available_checkpoint
    /// and highest_accumulated_checkpoint, continually and sequentially
    /// accumulates checkpoint state hashes to the root state hash.
    ///
    /// Proof of concept: do a union of all checkpoint accumulators at the end of the epoch to generate the
    /// root state hash and save it.
    async fn accumulate(&self, epoch: EpochId) -> Result<(), TypedStoreError> {
        // TODO -- verify that we are halted for epoch change

        let next_to_accumulate = self
            .store
            .get_highest_accumulated_checkpoint_seq_number()?
            .map(|num| num.saturating_add(1))
            .unwrap_or(0);
        let highest_hashed = self
            .store
            .get_highest_hashed_checkpoint_seq_number()?
            .expect("At end of epoch highest hashed cannot be None");

        let mut root_state_hash = self
            .store
            .root_state_hash
            .get(&ROOT_STATE_HASH_KEY)?
            .unwrap_or_default();

        for i in next_to_accumulate..=highest_hashed {
            let acc = self
                .store
                .state_hash_by_checkpoint
                .get(&i)?
                .unwrap_or_else(|| panic!("Accumulator for checkpoint sequence number {i:?} not present in store"));
            root_state_hash.union(&acc);
        }

        // Update watermark, root_state_hash, and digest atomically
        let root_state_digest = root_state_hash.digest();

        let batch = self.store.root_state_hash.batch();

        let batch = batch.insert_batch(
            &self.store.root_state_hash,
            std::iter::once((&ROOT_STATE_HASH_KEY, root_state_hash)),
        )?;
        let batch = batch.insert_batch(
            &self.store.root_state_digest_by_epoch,
            std::iter::once((&epoch, root_state_digest)),
        )?;
        let batch = batch.insert_batch(
            &self.store.watermarks,
            std::iter::once((&CheckpointWatermark::HighestAccumulated, highest_hashed)),
        )?;

        batch.write()?;

        Ok(())
    }
}

fn hash_checkpoint_effects(state: State, store: Arc<StateAccumulatorStore>) -> SuiResult<Accumulator> {
    let seq_num = state.checkpoint_seq_num;

    if store.state_hash_by_checkpoint.contains_key(&seq_num).unwrap() {
        return Err(SuiError::from(format!("StateAccumulator: checkpoint already hashed: {seq_num:?}").as_str()));
    }

    let mut acc = Accumulator::default();

    acc.insert_all(
        state.effects.iter().flat_map(
            |fx| fx.created.clone().into_iter().map(|(obj_ref, _)| obj_ref.2).collect::<Vec<ObjectDigest>>()
        ).collect::<Vec<ObjectDigest>>()
    );
    acc.remove_all(
        state.effects.iter().flat_map(
            |fx| fx.deleted.clone().into_iter().map(|obj_ref| obj_ref.2).collect::<Vec<ObjectDigest>>()
        ).collect::<Vec<ObjectDigest>>()
    );
    // TODO almost certainly not currectly handling "mutated" effects. Help?
    acc.insert_all(
        state.effects.iter().flat_map(
            |fx| fx.mutated.clone().into_iter().map(|(obj_ref, _)| obj_ref.2).collect::<Vec<ObjectDigest>>()
        ).collect::<Vec<ObjectDigest>>()
    );
    Ok(acc)
}
