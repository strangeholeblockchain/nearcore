use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cached::{Cached, SizedCache};
use conqueue::{QueueReceiver, QueueSender};
#[cfg(feature = "test_features")]
use serde::{Deserialize, Serialize};
use tracing::warn;

use near_primitives::hash::{hash, CryptoHash};
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use near_store::{ColAccountAnnouncements, Store};

use crate::PeerInfo;
use crate::{
    cache::RouteBackCache,
    types::{PeerIdOrHash, Ping, Pong},
    utils::cache_to_hashmap,
};
use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Message};
use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, WriteBytesExt};
use near_crypto::{KeyType, SecretKey, Signature};
use std::hash::{Hash, Hasher};

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const ROUTE_BACK_CACHE_SIZE: u64 = 100_000;
const ROUTE_BACK_CACHE_EVICT_TIMEOUT: u64 = 120_000; // 120 seconds
const ROUTE_BACK_CACHE_REMOVE_BATCH: u64 = 100;
const PING_PONG_CACHE_SIZE: usize = 1_000;
const ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED: usize = 10;
const ROUND_ROBIN_NONCE_CACHE_SIZE: usize = 10_000;
/// Routing table will clean edges if there is at least one node that is not reachable
/// since `SAVE_PEERS_MAX_TIME` seconds. All peers disconnected since `SAVE_PEERS_AFTER_TIME`
/// seconds will be removed from cache and persisted in disk.
pub const SAVE_PEERS_MAX_TIME: u64 = 7_200;
pub const SAVE_PEERS_AFTER_TIME: u64 = 3_600;
/// Graph implementation supports up to 128 peers.
pub const MAX_NUM_PEERS: usize = 128;

/// Information that will be ultimately used to create a new edge.
/// It contains nonce proposed for the edge with signature from peer.
#[derive(Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Default)]
pub struct EdgeInfo {
    pub nonce: u64,
    pub signature: Signature,
}

impl EdgeInfo {
    pub fn new(peer0: PeerId, peer1: PeerId, nonce: u64, secret_key: &SecretKey) -> Self {
        let (peer0, peer1) = Edge::key(peer0, peer1);
        let data = Edge::build_hash(&peer0, &peer1, nonce);
        let signature = secret_key.sign(data.as_ref());
        Self { nonce, signature }
    }
}

/// Status of the edge
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, Hash)]
pub enum EdgeType {
    Added,
    Removed,
}

/// Edge object. Contains information relative to a new edge that is being added or removed
/// from the network. This is the information that is required.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "test_features", derive(Serialize, Deserialize))]
pub struct Edge {
    /// Since edges are not directed `peer0 < peer1` should hold.
    pub peer0: PeerId,
    pub peer1: PeerId,
    /// Nonce to keep tracking of the last update on this edge.
    /// It must be even
    pub nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    signature0: Signature,
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
    removal_info: Option<(bool, Signature)>,
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.peer0.0.try_to_vec().unwrap());
        state.write(&self.peer1.0.try_to_vec().unwrap());
        state.write_u64(self.nonce)
    }
}

impl Edge {
    /// Create an addition edge.
    pub fn new(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        signature0: Signature,
        signature1: Signature,
    ) -> Self {
        let (peer0, signature0, peer1, signature1) = if peer0 < peer1 {
            (peer0, signature0, peer1, signature1)
        } else {
            (peer1, signature1, peer0, signature0)
        };

        Self { peer0, peer1, nonce, signature0, signature1, removal_info: None }
    }

    pub fn to_simple_edge(&self) -> SimpleEdge {
        SimpleEdge::new(self.peer0.clone(), self.peer1.clone(), self.nonce)
    }

    pub fn make_fake_edge(peer0: PeerId, peer1: PeerId, nonce: u64) -> Self {
        Self {
            peer0,
            peer1,
            nonce,
            signature0: Signature::empty(KeyType::ED25519),
            signature1: Signature::empty(KeyType::ED25519),
            removal_info: None,
        }
    }

    /// Build a new edge with given information from the other party.
    pub fn build_with_secret_key(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        secret_key: &SecretKey,
        signature1: Signature,
    ) -> Self {
        let hash = if peer0 < peer1 {
            Edge::build_hash(&peer0, &peer1, nonce)
        } else {
            Edge::build_hash(&peer1, &peer0, nonce)
        };
        let signature0 = secret_key.sign(hash.as_ref());
        Edge::new(peer0, peer1, nonce, signature0, signature1)
    }

    /// Create the remove edge change from an added edge change.
    pub fn remove_edge(&self, me: PeerId, sk: &SecretKey) -> Self {
        assert_eq!(self.edge_type(), EdgeType::Added);
        let mut edge = self.clone();
        edge.nonce += 1;
        let me = edge.peer0 == me;
        let hash = edge.hash();
        let signature = sk.sign(hash.as_ref());
        edge.removal_info = Some((me, signature));
        edge
    }

    /// Build the hash of the edge given its content.
    /// It is important that peer0 < peer1 at this point.
    fn build_hash(peer0: &PeerId, peer1: &PeerId, nonce: u64) -> CryptoHash {
        let mut buffer = Vec::<u8>::new();
        let peer0: Vec<u8> = peer0.clone().into();
        buffer.extend_from_slice(peer0.as_slice());
        let peer1: Vec<u8> = peer1.clone().into();
        buffer.extend_from_slice(peer1.as_slice());
        buffer.write_u64::<LittleEndian>(nonce).unwrap();
        hash(buffer.as_slice())
    }

    fn hash(&self) -> CryptoHash {
        Edge::build_hash(&self.peer0, &self.peer1, self.nonce)
    }

    fn prev_hash(&self) -> CryptoHash {
        Edge::build_hash(&self.peer0, &self.peer1, self.nonce - 1)
    }

    pub fn verify(&self) -> bool {
        if self.peer0 > self.peer1 {
            return false;
        }

        match self.edge_type() {
            EdgeType::Added => {
                let data = self.hash();

                self.removal_info.is_none()
                    && self.signature0.verify(data.as_ref(), &self.peer0.public_key())
                    && self.signature1.verify(data.as_ref(), &self.peer1.public_key())
            }
            EdgeType::Removed => {
                // nonce should be an even positive number
                if self.nonce == 0 {
                    return false;
                }

                // Check referring added edge is valid.
                let add_hash = self.prev_hash();
                if !self.signature0.verify(add_hash.as_ref(), &self.peer0.public_key())
                    || !self.signature1.verify(add_hash.as_ref(), &self.peer1.public_key())
                {
                    return false;
                }

                if let Some((party, signature)) = &self.removal_info {
                    let peer = if *party { &self.peer0 } else { &self.peer1 };
                    let del_hash = self.hash();
                    signature.verify(del_hash.as_ref(), &peer.public_key())
                } else {
                    false
                }
            }
        }
    }

    pub fn key(peer0: PeerId, peer1: PeerId) -> (PeerId, PeerId) {
        if peer0 < peer1 {
            (peer0, peer1)
        } else {
            (peer1, peer0)
        }
    }

    /// Helper function when adding a new edge and we receive information from new potential peer
    /// to verify the signature.
    pub fn partial_verify(peer0: PeerId, peer1: PeerId, edge_info: &EdgeInfo) -> bool {
        let pk = peer1.public_key();
        let (peer0, peer1) = Edge::key(peer0, peer1);
        let data = Edge::build_hash(&peer0, &peer1, edge_info.nonce);
        edge_info.signature.verify(data.as_ref(), &pk)
    }

    pub fn get_pair(&self) -> (PeerId, PeerId) {
        (self.peer0.clone(), self.peer1.clone())
    }

    /// It will be considered as a new edge if the nonce is odd, otherwise it is canceling the
    /// previous edge.
    pub fn edge_type(&self) -> EdgeType {
        if self.nonce % 2 == 1 {
            EdgeType::Added
        } else {
            EdgeType::Removed
        }
    }

    /// Next nonce of valid addition edge.
    pub fn next_nonce(nonce: u64) -> u64 {
        if nonce % 2 == 1 {
            nonce + 2
        } else {
            nonce + 1
        }
    }

    /// Next nonce of valid addition edge.
    pub fn next(&self) -> u64 {
        Edge::next_nonce(self.nonce)
    }

    pub fn contains_peer(&self, peer_id: &PeerId) -> bool {
        self.peer0 == *peer_id || self.peer1 == *peer_id
    }

    /// Find a peer id in this edge different from `me`.
    pub fn other(&self, me: &PeerId) -> Option<PeerId> {
        if self.peer0 == *me {
            Some(self.peer1.clone())
        } else if self.peer1 == *me {
            Some(self.peer0.clone())
        } else {
            None
        }
    }
}

/// Represents edge between two nodes. Unlike `Edge` it doesn't contain signatures.
#[derive(Hash, Clone, Eq, PartialEq, Debug)]
#[cfg_attr(feature = "test_features", derive(Serialize, Deserialize))]
pub struct SimpleEdge {
    key: (PeerId, PeerId),
    nonce: u64,
}

impl SimpleEdge {
    pub fn new(peer0: PeerId, peer1: PeerId, nonce: u64) -> SimpleEdge {
        let (peer0, peer1) = Edge::key(peer0, peer1);
        SimpleEdge { key: (peer0, peer1), nonce }
    }

    pub fn key(&self) -> &(PeerId, PeerId) {
        &self.key
    }

    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    pub fn edge_type(&self) -> EdgeType {
        if self.nonce % 2 == 1 {
            EdgeType::Added
        } else {
            EdgeType::Removed
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, Copy)]
pub struct ValidIBFLevel(pub u64);

/// We create IbfSets of various sizes from 2^10+2 up to 2^17+2. Those constants specify valid ranges.
pub const MIN_IBF_LEVEL: ValidIBFLevel = ValidIBFLevel(10);
pub const MAX_IBF_LEVEL: ValidIBFLevel = ValidIBFLevel(17);

/// Represents IbfLevel from 10 to 17.
impl ValidIBFLevel {
    pub fn inc(&self) -> Option<ValidIBFLevel> {
        if self.0 + 1 >= MIN_IBF_LEVEL.0 && self.0 + 1 <= MAX_IBF_LEVEL.0 {
            Some(ValidIBFLevel(self.0 + 1))
        } else {
            None
        }
    }

    pub fn is_valid(&self) -> bool {
        return self.0 >= MIN_IBF_LEVEL.0 && self.0 <= MAX_IBF_LEVEL.0;
    }
}

#[cfg_attr(feature = "test_features", derive(Serialize))]
pub struct PeerRequestResult {
    pub peers: Vec<PeerInfo>,
}

impl<A, M> MessageResponse<A, M> for PeerRequestResult
where
    A: Actor,
    M: Message<Result = PeerRequestResult>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[derive(MessageResponse, Debug)]
#[cfg_attr(feature = "test_features", derive(Serialize))]
pub struct SetAdvOptionsResult {}

#[derive(MessageResponse, Debug)]
#[cfg_attr(feature = "test_features", derive(Serialize))]
pub struct GetRoutingTableResult {
    pub edges_info: Vec<SimpleEdge>,
}

pub struct EdgeVerifierHelper {
    /// Shared version of edges_info used by multiple threads
    pub edges_info_shared: Arc<Mutex<HashMap<(PeerId, PeerId), u64>>>,
    /// Queue of edges verified, but not added yes
    pub edges_to_add_receiver: QueueReceiver<Edge>,
    pub edges_to_add_sender: QueueSender<Edge>,
}

impl Default for EdgeVerifierHelper {
    fn default() -> Self {
        let (tx, rx) = conqueue::Queue::unbounded::<Edge>();
        Self {
            edges_info_shared: Default::default(),
            edges_to_add_sender: tx,
            edges_to_add_receiver: rx,
        }
    }
}

pub struct RoutingTable {
    /// PeerId associated for every known account id.
    account_peers: SizedCache<AccountId, AnnounceAccount>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: HashMap<PeerId, Vec<PeerId>>,
    /// Store last update for known edges.
    // TODO (PIOTR, #4859) Change this to store only edges of all peers that we are connected to.
    pub edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Hash of messages that requires routing back to respective previous hop.
    pub route_back: RouteBackCache,
    /// Access to store on disk
    store: Arc<Store>,
    /// Number of times each active connection was used to route a message.
    /// If there are several options use route with minimum nonce.
    /// New routes are added with minimum nonce.
    route_nonce: SizedCache<PeerId, usize>,
    /// Ping received by nonce.
    ping_info: SizedCache<usize, (Ping, usize)>,
    /// Ping received by nonce.
    pong_info: SizedCache<usize, (Pong, usize)>,
    /// List of pings sent for which we haven't received any pong yet.
    waiting_pong: SizedCache<PeerId, SizedCache<usize, Instant>>,
    /// Last nonce sent to each peer through pings.
    last_ping_nonce: SizedCache<PeerId, usize>,
}

#[derive(Debug)]
pub enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
    RouteBackNotFound,
}

impl RoutingTable {
    pub fn new(store: Arc<Store>) -> Self {
        // Find greater nonce on disk and set `component_nonce` to this value.

        Self {
            account_peers: SizedCache::with_size(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            peer_forwarding: Default::default(),
            edges_info: Default::default(),
            route_back: RouteBackCache::new(
                ROUTE_BACK_CACHE_SIZE,
                ROUTE_BACK_CACHE_EVICT_TIMEOUT,
                ROUTE_BACK_CACHE_REMOVE_BATCH,
            ),
            store,
            route_nonce: SizedCache::with_size(ROUND_ROBIN_NONCE_CACHE_SIZE),
            ping_info: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            pong_info: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            waiting_pong: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            last_ping_nonce: SizedCache::with_size(PING_PONG_CACHE_SIZE),
        }
    }

    pub fn reachable_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peer_forwarding.keys()
    }

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to `peer_id`.
    pub fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        if let Some(routes) = self.peer_forwarding.get(&peer_id).cloned() {
            if routes.is_empty() {
                return Err(FindRouteError::Disconnected);
            }

            // Strategy similar to Round Robin. Select node with least nonce and send it. Increase its
            // nonce by one. Additionally if the difference between the highest nonce and the lowest
            // nonce is greater than some threshold increase the lowest nonce to be at least
            // max nonce - threshold.
            let nonce_peer = routes
                .iter()
                .map(|peer_id| {
                    (self.route_nonce.cache_get(&peer_id).cloned().unwrap_or(0), peer_id)
                })
                .collect::<Vec<_>>();

            // Neighbor with minimum and maximum nonce respectively.
            let min_v = nonce_peer.iter().min().cloned().unwrap();
            let max_v = nonce_peer.into_iter().max().unwrap();

            if min_v.0 + ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED < max_v.0 {
                self.route_nonce
                    .cache_set(min_v.1.clone(), max_v.0 - ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED);
            }

            let next_hop = min_v.1;
            let nonce = self.route_nonce.cache_get(&next_hop).cloned();
            self.route_nonce.cache_set(next_hop.clone(), nonce.map_or(1, |nonce| nonce + 1));
            Ok(next_hop.clone())
        } else {
            Err(FindRouteError::PeerNotFound)
        }
    }

    pub fn find_route(&mut self, target: &PeerIdOrHash) -> Result<PeerId, FindRouteError> {
        match target {
            PeerIdOrHash::PeerId(peer_id) => self.find_route_from_peer_id(&peer_id),
            PeerIdOrHash::Hash(hash) => {
                self.fetch_route_back(hash.clone()).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    /// Find peer that owns this AccountId.
    pub fn account_owner(&mut self, account_id: &AccountId) -> Result<PeerId, FindRouteError> {
        self.get_announce(account_id)
            .map(|announce_account| announce_account.peer_id)
            .ok_or_else(|| FindRouteError::AccountNotFound)
    }

    /// Add (account id, peer id) to routing table.
    /// Note: There is at most on peer id per account id.
    pub fn add_account(&mut self, announce_account: AnnounceAccount) {
        let account_id = announce_account.account_id.clone();
        self.account_peers.cache_set(account_id.clone(), announce_account.clone());

        // Add account to store
        let mut update = self.store.store_update();
        if let Err(e) = update
            .set_ser(ColAccountAnnouncements, account_id.as_ref().as_bytes(), &announce_account)
            .and_then(|_| update.commit())
        {
            warn!(target: "network", "Error saving announce account to store: {:?}", e);
        }
    }

    // TODO(MarX, #1694): Allow one account id to be routed to several peer id.
    pub fn contains_account(&mut self, announce_account: &AnnounceAccount) -> bool {
        self.get_announce(&announce_account.account_id).map_or(false, |current_announce_account| {
            current_announce_account.epoch_id == announce_account.epoch_id
        })
    }

    pub fn remove_edges(&mut self, edges: &Vec<Edge>) {
        for edge in edges.iter() {
            let key = (edge.peer0.clone(), edge.peer1.clone());
            self.edges_info.remove(&key);
        }
    }

    pub fn add_route_back(&mut self, hash: CryptoHash, peer_id: PeerId) {
        self.route_back.insert(hash, peer_id);
    }

    // Find route back with given hash and removes it from cache.
    fn fetch_route_back(&mut self, hash: CryptoHash) -> Option<PeerId> {
        self.route_back.remove(&hash)
    }

    pub fn compare_route_back(&mut self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub fn add_ping(&mut self, ping: Ping) {
        let cnt = self.ping_info.cache_get(&(ping.nonce as usize)).map(|v| v.1).unwrap_or(0);

        self.ping_info.cache_set(ping.nonce as usize, (ping, cnt + 1));
    }

    /// Return time of the round trip of ping + pong
    pub fn add_pong(&mut self, pong: Pong) -> Option<f64> {
        let mut res = None;

        if let Some(nonces) = self.waiting_pong.cache_get_mut(&pong.source) {
            res = nonces
                .cache_remove(&(pong.nonce as usize))
                .and_then(|sent| Some(Instant::now().duration_since(sent).as_secs_f64() * 1000f64));
        }

        let cnt = self.pong_info.cache_get(&(pong.nonce as usize)).map(|v| v.1).unwrap_or(0);

        self.pong_info.cache_set(pong.nonce as usize, (pong, (cnt + 1)));

        res
    }

    // for unit tests
    pub fn sending_ping(&mut self, nonce: usize, target: PeerId) {
        let entry = if let Some(entry) = self.waiting_pong.cache_get_mut(&target) {
            entry
        } else {
            self.waiting_pong.cache_set(target.clone(), SizedCache::with_size(10));
            self.waiting_pong.cache_get_mut(&target).unwrap()
        };

        entry.cache_set(nonce, Instant::now());
    }

    pub fn get_ping(&mut self, peer_id: PeerId) -> usize {
        if let Some(entry) = self.last_ping_nonce.cache_get_mut(&peer_id) {
            *entry += 1;
            *entry - 1
        } else {
            self.last_ping_nonce.cache_set(peer_id, 1);
            0
        }
    }

    // for unit tests
    pub fn fetch_ping_pong(
        &self,
    ) -> (HashMap<usize, (Ping, usize)>, HashMap<usize, (Pong, usize)>) {
        (cache_to_hashmap(&self.ping_info), cache_to_hashmap(&self.pong_info))
    }

    pub fn info(&mut self) -> RoutingTableInfo {
        let account_peers = self
            .get_announce_accounts()
            .into_iter()
            .map(|announce_account| (announce_account.account_id, announce_account.peer_id))
            .collect();
        RoutingTableInfo { account_peers, peer_forwarding: self.peer_forwarding.clone() }
    }
    /// Public interface for `account_peers`
    ///
    /// Get keys currently on cache.
    pub fn get_accounts_keys(&mut self) -> Vec<AccountId> {
        self.account_peers.key_order().cloned().collect()
    }

    /// Get announce accounts on cache.
    pub fn get_announce_accounts(&mut self) -> Vec<AnnounceAccount> {
        self.account_peers.value_order().cloned().collect()
    }

    /// Get number of accounts
    pub fn get_announce_accounts_size(&mut self) -> usize {
        self.account_peers.cache_size()
    }

    /// Get account announce from
    pub fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(announce_account) = self.account_peers.cache_get(&account_id) {
            Some(announce_account.clone())
        } else {
            self.store
                .get_ser(ColAccountAnnouncements, account_id.as_ref().as_bytes())
                .and_then(|res: Option<AnnounceAccount>| {
                    if let Some(announce_account) = res {
                        self.add_account(announce_account.clone());
                        Ok(Some(announce_account))
                    } else {
                        Ok(None)
                    }
                })
                .unwrap_or_else(|e| {
                    warn!(target: "network", "Error loading announce account from store: {:?}", e);
                    None
                })
        }
    }

    pub fn get_edge(&self, peer0: PeerId, peer1: PeerId) -> Option<Edge> {
        let key = Edge::key(peer0, peer1);
        self.edges_info.get(&key).cloned()
    }
}
#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub peer_forwarding: HashMap<PeerId, Vec<PeerId>>,
}

#[derive(Clone)]
pub struct Graph {
    pub source: PeerId,
    source_id: u32,
    p2id: HashMap<PeerId, u32>,
    id2p: Vec<PeerId>,
    pub used: Vec<bool>,
    pub unused: Vec<u32>,
    adjacency: Vec<Vec<u32>>,

    pub total_active_edges: u64,
}

impl Graph {
    pub fn new(source: PeerId) -> Self {
        let mut res = Self {
            source: source.clone(),
            source_id: 0,
            p2id: HashMap::default(),
            id2p: Vec::default(),
            used: Vec::default(),
            unused: Vec::default(),
            adjacency: Vec::default(),
            total_active_edges: 0,
        };
        res.id2p.push(source.clone());
        res.adjacency.push(Vec::default());
        res.p2id.insert(source, res.source_id);
        res.used.push(true);

        res
    }

    fn contains_edge(&self, peer0: &PeerId, peer1: &PeerId) -> bool {
        if let Some(&id0) = self.p2id.get(&peer0) {
            if let Some(&id1) = self.p2id.get(&peer1) {
                return self.adjacency[id0 as usize].contains(&id1);
            }
        }
        false
    }

    fn remove_if_unused(&mut self, id: u32) {
        let entry = &self.adjacency[id as usize];

        if entry.is_empty() && id != self.source_id {
            self.used[id as usize] = false;
            self.unused.push(id);
            self.p2id.remove(&self.id2p[id as usize]);
        }
    }

    fn get_id(&mut self, peer: &PeerId) -> u32 {
        match self.p2id.entry(peer.clone()) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let val = if let Some(val) = self.unused.pop() {
                    assert!(!self.used[val as usize]);
                    assert!(self.adjacency[val as usize].is_empty());
                    self.id2p[val as usize] = peer.clone();
                    self.used[val as usize] = true;
                    val
                } else {
                    let val = self.id2p.len() as u32;
                    self.id2p.push(peer.clone());
                    self.used.push(true);
                    self.adjacency.push(Vec::default());
                    val
                };

                vacant.insert(val);
                val
            }
        }
    }

    pub fn add_edge(&mut self, peer0: PeerId, peer1: PeerId) {
        assert_ne!(peer0, peer1);
        if !self.contains_edge(&peer0, &peer1) {
            let id0 = self.get_id(&peer0);
            let id1 = self.get_id(&peer1);

            self.adjacency[id0 as usize].push(id1);
            self.adjacency[id1 as usize].push(id0);

            self.total_active_edges += 1;
        }
    }

    pub fn remove_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        assert_ne!(peer0, peer1);
        if self.contains_edge(&peer0, &peer1) {
            let id0 = self.get_id(&peer0);
            let id1 = self.get_id(&peer1);

            self.adjacency[id0 as usize].retain(|&x| x != id1);
            self.adjacency[id1 as usize].retain(|&x| x != id0);

            self.remove_if_unused(id0);
            self.remove_if_unused(id1);

            self.total_active_edges -= 1;
        }
    }

    /// Compute for every node `u` on the graph (other than `source`) which are the neighbors of
    /// `sources` which belong to the shortest path from `source` to `u`. Nodes that are
    /// not connected to `source` will not appear in the result.
    pub fn calculate_distance(&self) -> HashMap<PeerId, Vec<PeerId>> {
        // TODO add removal of unreachable nodes

        let mut queue = VecDeque::new();

        let nodes = self.id2p.len();
        let mut distance: Vec<i32> = vec![-1; nodes];
        let mut routes: Vec<u128> = vec![0; nodes];

        distance[self.source_id as usize] = 0;

        {
            let neighbors = &self.adjacency[self.source_id as usize];
            for (id, &neighbor) in neighbors.iter().enumerate().take(MAX_NUM_PEERS) {
                queue.push_back(neighbor);
                distance[neighbor as usize] = 1;
                routes[neighbor as usize] = 1u128 << id;
            }
        }

        while let Some(cur_peer) = queue.pop_front() {
            let cur_distance = distance[cur_peer as usize];

            for &neighbor in &self.adjacency[cur_peer as usize] {
                if distance[neighbor as usize] == -1 {
                    distance[neighbor as usize] = cur_distance + 1;
                    queue.push_back(neighbor);
                }
                // If this edge belong to a shortest path, all paths to
                // the closer nodes are also valid for the current node.
                if distance[neighbor as usize] == cur_distance + 1 {
                    routes[neighbor as usize] |= routes[cur_peer as usize];
                }
            }
        }

        self.compute_result(&mut routes, &distance)
    }

    fn compute_result(&self, routes: &[u128], distance: &[i32]) -> HashMap<PeerId, Vec<PeerId>> {
        let mut res = HashMap::with_capacity(routes.len());

        let neighbors = &self.adjacency[self.source_id as usize];
        let mut unreachable_nodes = 0;

        for (key, &cur_route) in routes.iter().enumerate() {
            if distance[key] == -1 && self.used[key] {
                unreachable_nodes += 1;
            }
            if key as u32 == self.source_id
                || distance[key] == -1
                || cur_route == 0u128
                || !self.used[key]
            {
                continue;
            }
            let mut peer_set: Vec<PeerId> = Vec::with_capacity(cur_route.count_ones() as usize);

            for (id, &neighbor) in neighbors.iter().enumerate().take(MAX_NUM_PEERS) {
                if (cur_route & (1u128 << id)) != 0 {
                    peer_set.push(self.id2p[neighbor as usize].clone());
                };
            }
            res.insert(self.id2p[key].clone(), peer_set);
        }
        if unreachable_nodes > 1000 {
            warn!("We store more than 1000 unreachable nodes: {}", unreachable_nodes);
        }
        res
    }
}

#[cfg(test)]
mod test {
    use crate::routing::Graph;
    use crate::test_utils::{expected_routing_tables, random_peer_id};

    #[test]
    fn graph_contains_edge() {
        let source = random_peer_id();

        let node0 = random_peer_id();
        let node1 = random_peer_id();

        let mut graph = Graph::new(source.clone());

        assert_eq!(graph.contains_edge(&source, &node0), false);
        assert_eq!(graph.contains_edge(&source, &node1), false);
        assert_eq!(graph.contains_edge(&node0, &node1), false);
        assert_eq!(graph.contains_edge(&node1, &node0), false);

        graph.add_edge(node0.clone(), node1.clone());

        assert_eq!(graph.contains_edge(&source, &node0), false);
        assert_eq!(graph.contains_edge(&source, &node1), false);
        assert_eq!(graph.contains_edge(&node0, &node1), true);
        assert_eq!(graph.contains_edge(&node1, &node0), true);

        graph.remove_edge(&node1, &node0);

        assert_eq!(graph.contains_edge(&node0, &node1), false);
        assert_eq!(graph.contains_edge(&node1, &node0), false);
    }

    #[test]
    fn graph_distance0() {
        let source = random_peer_id();
        let node0 = random_peer_id();

        let mut graph = Graph::new(source.clone());
        graph.add_edge(source.clone(), node0.clone());
        graph.remove_edge(&source, &node0);
        graph.add_edge(source.clone(), node0.clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![(node0.clone(), vec![node0.clone()])],
        ));
    }

    #[test]
    fn graph_distance1() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());

        assert!(expected_routing_tables(graph.calculate_distance(), vec![]));
    }

    #[test]
    fn graph_distance2() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());
        graph.add_edge(source.clone(), nodes[0].clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[0].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone()]),
            ],
        ));
    }

    #[test]
    fn graph_distance3() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[0].clone(), nodes[2].clone());
        graph.add_edge(source.clone(), nodes[0].clone());
        graph.add_edge(source.clone(), nodes[1].clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[1].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone(), nodes[1].clone()]),
            ],
        ));
    }

    /// Test the following graph
    ///     0 - 3 - 6
    ///   /   x   x
    /// s - 1 - 4 - 7
    ///   \   x   x
    ///     2 - 5 - 8
    ///
    ///    9 - 10 (Dummy edge disconnected)
    ///
    /// There is a shortest path to nodes [3..9) going through 0, 1, and 2.
    #[test]
    fn graph_distance4() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..11).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        for i in 0..3 {
            graph.add_edge(source.clone(), nodes[i].clone());
        }

        for level in 0..2 {
            for i in 0..3 {
                for j in 0..3 {
                    graph.add_edge(nodes[level * 3 + i].clone(), nodes[level * 3 + 3 + j].clone());
                }
            }
        }

        // Dummy edge.
        graph.add_edge(nodes[9].clone(), nodes[10].clone());

        let mut next_hops: Vec<_> =
            (0..3).map(|i| (nodes[i].clone(), vec![nodes[i].clone()])).collect();
        let target: Vec<_> = (0..3).map(|i| nodes[i].clone()).collect();

        for i in 3..9 {
            next_hops.push((nodes[i].clone(), target.clone()));
        }

        assert!(expected_routing_tables(graph.calculate_distance(), next_hops));
    }
}
