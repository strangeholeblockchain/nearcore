use std::collections::{HashMap, HashSet};

use actix::dev::MessageResponse;
use actix::{Actor, Addr, Context, Handler, Message, System};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use tracing::error;
use tracing::{debug, trace, warn};

use crate::metrics;
use near_performance_metrics_macros::perf;
use near_primitives::network::PeerId;

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::ibf::{Ibf, IbfBox};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::ibf_peer_set::IbfPeerSet;
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::ibf_set::IbfSet;
use crate::routing::{Edge, EdgeType, Graph, ProcessEdgeResult, SAVE_PEERS_MAX_TIME};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::routing::{SimpleEdge, ValidIBFLevel, MIN_IBF_LEVEL};
use crate::types::StopMsg;
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::types::{PartialSync, PeerMessage, RoutingState, RoutingSyncV2, RoutingVersion2};
#[cfg(feature = "delay_detector")]
use delay_detector::DelayDetector;
use near_primitives::utils::index_to_bytes;
use near_store::db::DBCol::{ColComponentEdges, ColLastComponentNonce, ColPeerComponent};
use near_store::{Store, StoreUpdate};
use std::ops::Sub;
use std::sync::Arc;

/// Actor that maintains routing table information.
/// TODO (PIOTR, #4859) Finish moving routing table computation to new thread.
pub struct RoutingTableActor {
    /// Data structures with all edges.
    pub edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Data structure used for exchanging routing tables.
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    pub peer_ibf_set: IbfPeerSet,
    /// Current view of the network. Nodes are Peers and edges are active connections.
    pub raw_graph: Graph,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: HashMap<PeerId, Vec<PeerId>>,
    /// Last time a peer with reachable through active edges.
    pub peer_last_time_reachable: HashMap<PeerId, chrono::DateTime<chrono::Utc>>,
    /// Access to store on disk
    store: Arc<Store>,
    /// Last nonce used to store edges on disk.
    pub component_nonce: u64,
}

#[derive(Debug)]
pub struct AddVerifiedEdgesResponse {
    pub new_edge: bool,
    pub added_edges: Vec<Edge>,
}

impl RoutingTableActor {
    pub fn new(peer_id: PeerId, store: Arc<Store>) -> Self {
        let component_nonce = store
            .get_ser::<u64>(ColLastComponentNonce, &[])
            .unwrap_or(None)
            .map_or(0, |nonce| nonce + 1);
        Self {
            edges_info: Default::default(),
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            peer_ibf_set: Default::default(),
            raw_graph: Graph::new(peer_id),
            peer_forwarding: Default::default(),
            peer_last_time_reachable: Default::default(),
            store,
            component_nonce,
        }
    }

    #[cfg(feature = "test_features")]
    pub fn remove_edges(&mut self, edges: &[Edge]) {
        for edge in edges.iter() {
            self.remove_edge(edge);
        }
    }

    pub fn remove_edge(&mut self, edge: &Edge) {
        #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
        self.peer_ibf_set.remove_edge(&edge.to_simple_edge());

        let key = (edge.peer0.clone(), edge.peer1.clone());
        if self.edges_info.remove(&key).is_some() {
            self.raw_graph.remove_edge(&edge.peer0, &edge.peer1);
        }
    }

    fn add_edge(&mut self, edge: Edge) -> bool {
        let key = edge.get_pair();
        if self.find_nonce(&key) >= edge.nonce {
            // We already have a newer information about this edge. Discard this information.
            false
        } else {
            match edge.edge_type() {
                EdgeType::Added => {
                    self.raw_graph.add_edge(key.0.clone(), key.1.clone());
                }
                EdgeType::Removed => {
                    self.raw_graph.remove_edge(&key.0, &key.1);
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            self.peer_ibf_set.add_edge(&edge.to_simple_edge());
            self.edges_info.insert(key, edge);
            true
        }
    }

    /// Add several edges to the current view of the network.
    /// These edges are assumed to be valid at this point.
    /// Return true if some of the edges contains new information to the network.
    pub fn process_edges(&mut self, edges: Vec<Edge>) -> ProcessEdgeResult {
        let mut new_edge = false;
        let total = edges.len();
        let mut result = Vec::with_capacity(edges.len() as usize);

        for edge in edges {
            let key = edge.get_pair();

            self.touch(&key.0);
            self.touch(&key.1);

            if self.add_edge(edge.clone()) {
                new_edge = true;
                result.push(edge);
            }
        }

        // Update metrics after edge update
        near_metrics::inc_counter_by(&metrics::EDGE_UPDATES, total as u64);
        near_metrics::set_gauge(&metrics::EDGE_ACTIVE, self.raw_graph.total_active_edges as i64);

        ProcessEdgeResult { new_edge, edges: result }
    }

    /// If peer_id is not on memory check if it is on disk in bring it back on memory.
    fn touch(&mut self, peer_id: &PeerId) {
        if peer_id == self.peer_id() || self.peer_last_time_reachable.contains_key(peer_id) {
            return;
        }

        let me = self.peer_id().clone();

        if let Ok(nonce) = self.component_nonce_from_peer(peer_id.clone()) {
            let mut update = self.store.store_update();

            if let Ok(edges) = self.get_component_edges(nonce, &mut update) {
                for edge in edges {
                    for &peer_id in vec![&edge.peer0, &edge.peer1].iter() {
                        if peer_id == &me || self.peer_last_time_reachable.contains_key(peer_id) {
                            continue;
                        }

                        if let Ok(cur_nonce) = self.component_nonce_from_peer(peer_id.clone()) {
                            if cur_nonce == nonce {
                                self.peer_last_time_reachable.insert(
                                    peer_id.clone(),
                                    chrono::Utc::now()
                                        .sub(chrono::Duration::seconds(SAVE_PEERS_MAX_TIME as i64)),
                                );
                                update
                                    .delete(ColPeerComponent, Vec::from(peer_id.clone()).as_ref());
                            }
                        }
                    }
                    self.add_edge(edge);
                }
            }

            if let Err(e) = update.commit() {
                warn!(target: "network", "Error removing network component from store. {:?}", e);
            }
        } else {
            self.peer_last_time_reachable.insert(peer_id.clone(), chrono::Utc::now());
        }
    }

    fn peer_id(&self) -> &PeerId {
        &self.raw_graph.source
    }

    /// Add an edge update to the routing table and return if it is a new edge update.
    fn add_verified_edges_to_routing_table(
        &mut self,
        edges: Vec<Edge>,
    ) -> AddVerifiedEdgesResponse {
        let ProcessEdgeResult { new_edge, edges } = self.process_edges(edges);

        AddVerifiedEdgesResponse { new_edge, added_edges: edges }
    }

    /// Recalculate routing table.
    pub fn update(&mut self, can_save_edges: bool, force_pruning: bool, timeout: u64) -> Vec<Edge> {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("routing table update".into());
        let _routing_table_recalculation =
            near_metrics::start_timer(&metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM);

        trace!(target: "network", "Update routing table.");

        self.peer_forwarding = self.raw_graph.calculate_distance();

        let now = chrono::Utc::now();
        for peer in self.peer_forwarding.keys() {
            self.peer_last_time_reachable.insert(peer.clone(), now);
        }

        let mut edges_to_remove = Vec::new();
        if can_save_edges {
            edges_to_remove = self.try_save_edges(force_pruning, timeout);
        }

        near_metrics::inc_counter_by(&metrics::ROUTING_TABLE_RECALCULATIONS, 1);
        near_metrics::set_gauge(&metrics::PEER_REACHABLE, self.peer_forwarding.len() as i64);
        edges_to_remove
    }

    fn try_save_edges(&mut self, force_pruning: bool, timeout: u64) -> Vec<Edge> {
        let now = chrono::Utc::now();
        let mut oldest_time = now;
        let to_save = self
            .peer_last_time_reachable
            .iter()
            .filter_map(|(peer_id, last_time)| {
                oldest_time = std::cmp::min(oldest_time, *last_time);
                if now.signed_duration_since(*last_time).num_seconds() >= timeout as i64 {
                    Some(peer_id.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        // Save nodes on disk and remove from memory only if elapsed time from oldest peer
        // is greater than `SAVE_PEERS_MAX_TIME`
        if !force_pruning
            && now.signed_duration_since(oldest_time).num_seconds() < SAVE_PEERS_MAX_TIME as i64
        {
            return Vec::new();
        }
        debug!(target: "network", "try_save_edges: We are going to remove {} peers", to_save.len());

        let component_nonce = self.component_nonce;
        self.component_nonce += 1;

        let mut update = self.store.store_update();
        let _ = update.set_ser(ColLastComponentNonce, &[], &component_nonce);

        for peer_id in to_save.iter() {
            let _ = update.set_ser(
                ColPeerComponent,
                Vec::from(peer_id.clone()).as_ref(),
                &component_nonce,
            );

            self.peer_last_time_reachable.remove(peer_id);
        }

        let component_nonce = index_to_bytes(component_nonce);
        let mut edges_to_remove = vec![];

        self.edges_info.retain(|(peer0, peer1), edge| {
            if to_save.contains(peer0) || to_save.contains(peer1) {
                edges_to_remove.push(edge.clone());
                false
            } else {
                true
            }
        });

        let _ = update.set_ser(ColComponentEdges, component_nonce.as_ref(), &edges_to_remove);

        if let Err(e) = update.commit() {
            warn!(target: "network", "Error storing network component to store. {:?}", e);
        }
        edges_to_remove
    }

    fn update_and_remove_edges(
        &mut self,
        can_save_edges: bool,
        force_pruning: bool,
        timeout: u64,
    ) -> Vec<Edge> {
        let edges_to_remove = self.update(can_save_edges, force_pruning, timeout);
        edges_to_remove
    }

    pub fn find_nonce(&self, edge: &(PeerId, PeerId)) -> u64 {
        self.edges_info.get(&edge).map_or(0, |x| x.nonce)
    }

    pub fn get_edge(&self, peer0: PeerId, peer1: PeerId) -> Option<Edge> {
        let key = Edge::key(peer0, peer1);
        self.edges_info.get(&key).cloned()
    }

    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    pub fn get_edges_by_id(&self, edges: Vec<SimpleEdge>) -> Vec<Edge> {
        edges.iter().filter_map(|k| self.edges_info.get(&k.key()).cloned()).collect()
    }

    pub fn get_edges_len(&self) -> u64 {
        self.edges_info.len() as u64
    }

    /// Get the nonce of the component where the peer was stored
    fn component_nonce_from_peer(&mut self, peer_id: PeerId) -> Result<u64, ()> {
        match self.store.get_ser::<u64>(ColPeerComponent, Vec::from(peer_id).as_ref()) {
            Ok(Some(nonce)) => Ok(nonce),
            _ => Err(()),
        }
    }

    /// Get all edges in the component with `nonce`
    /// Remove those edges from the store.
    fn get_component_edges(
        &mut self,
        nonce: u64,
        update: &mut StoreUpdate,
    ) -> Result<Vec<Edge>, ()> {
        let enc_nonce = index_to_bytes(nonce);

        let res = match self.store.get_ser::<Vec<Edge>>(ColComponentEdges, enc_nonce.as_ref()) {
            Ok(Some(edges)) => Ok(edges),
            _ => Err(()),
        };

        update.delete(ColComponentEdges, enc_nonce.as_ref());

        res
    }
}

impl Actor for RoutingTableActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {}
}

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
impl RoutingTableActor {
    pub fn split_edges_for_peer(
        &self,
        peer_id: &PeerId,
        unknown_edges: &[u64],
    ) -> (Vec<SimpleEdge>, Vec<u64>) {
        self.peer_ibf_set.split_edges_for_peer(peer_id, unknown_edges)
    }
}

impl Handler<StopMsg> for RoutingTableActor {
    type Result = ();
    fn handle(&mut self, _: StopMsg, _ctx: &mut Self::Context) -> Self::Result {
        System::current().stop();
    }
}

#[derive(Debug)]
pub enum RoutingTableMessages {
    AddVerifiedEdges {
        edges: Vec<Edge>,
    },
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AdvRemoveEdges(Vec<Edge>),
    RequestRoutingTable,
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AddPeerIfMissing(PeerId, Option<u64>),
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    RemovePeer(PeerId),
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    ProcessIbfMessage {
        peer_id: PeerId,
        ibf_msg: RoutingVersion2,
    },
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSync {
        seed: u64,
    },
    RoutingTableUpdate {
        can_save_edges: bool,
        prune_edges: bool,
        timeout: u64,
    },
}

impl Message for RoutingTableMessages {
    type Result = RoutingTableMessagesResponse;
}

#[derive(MessageResponse, Debug)]
pub enum RoutingTableMessagesResponse {
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AddPeerResponse {
        seed: u64,
    },
    Empty,
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    ProcessIbfMessageResponse {
        ibf_msg: Option<RoutingVersion2>,
    },
    RequestRoutingTableResponse {
        edges_info: Vec<Edge>,
    },
    AddVerifiedEdgesResponse(AddVerifiedEdgesResponse),
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSyncResponse(PeerMessage),
    RoutingTableUpdateResponse {
        edges_to_remove: Vec<Edge>,
        /// Active PeerId that are part of the shortest path to each PeerId.
        peer_forwarding: HashMap<PeerId, Vec<PeerId>>,
    },
}

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
impl RoutingTableActor {
    pub fn exchange_routing_tables_using_ibf(
        &self,
        peer_id: &PeerId,
        ibf_set: &IbfSet<SimpleEdge>,
        ibf_level: ValidIBFLevel,
        ibf_vec: &[IbfBox],
        seed: u64,
    ) -> (Vec<SimpleEdge>, Vec<u64>, u64) {
        let ibf = ibf_set.get_ibf(ibf_level);

        let mut new_ibf = Ibf::from_vec(ibf_vec.clone(), seed ^ (ibf_level.0 as u64));

        if !new_ibf.merge(&ibf.data, seed ^ (ibf_level.0 as u64)) {
            error!(target: "network", "exchange routing tables failed with peer {}", peer_id);
            return (Default::default(), Default::default(), 0);
        }

        let (edge_hashes, unknown_edges_count) = new_ibf.try_recover();
        let (known, unknown_edges) = self.split_edges_for_peer(&peer_id, &edge_hashes);

        (known, unknown_edges, unknown_edges_count)
    }
}

impl Handler<RoutingTableMessages> for RoutingTableActor {
    type Result = RoutingTableMessagesResponse;

    #[perf]
    fn handle(&mut self, msg: RoutingTableMessages, _ctx: &mut Self::Context) -> Self::Result {
        debug!(target: "network", "RoutingTableMessages: {:?}", msg);
        match msg {
            RoutingTableMessages::AddVerifiedEdges { edges } => {
                RoutingTableMessagesResponse::AddVerifiedEdgesResponse(
                    self.add_verified_edges_to_routing_table(edges),
                )
            }
            RoutingTableMessages::RoutingTableUpdate { can_save_edges, prune_edges, timeout } => {
                let edges_to_remove =
                    self.update_and_remove_edges(can_save_edges, prune_edges, timeout);
                RoutingTableMessagesResponse::RoutingTableUpdateResponse {
                    edges_to_remove,
                    peer_forwarding: self.peer_forwarding.clone(),
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::StartRoutingTableSync { seed } => {
                RoutingTableMessagesResponse::StartRoutingTableSyncResponse(
                    PeerMessage::RoutingTableSyncV2(RoutingSyncV2::Version2(RoutingVersion2 {
                        known_edges: self.edges_info.len() as u64,
                        seed,
                        edges: Default::default(),
                        routing_state: RoutingState::InitializeIbf,
                    })),
                )
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::AdvRemoveEdges(edges) => {
                for edge in edges.iter() {
                    self.remove_edge(edge);
                }
                RoutingTableMessagesResponse::Empty
            }
            RoutingTableMessages::RequestRoutingTable => {
                RoutingTableMessagesResponse::RequestRoutingTableResponse {
                    edges_info: self.edges_info.iter().map(|(_k, v)| v.clone()).collect(),
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::AddPeerIfMissing(peer_id, ibf_set) => {
                let seed =
                    self.peer_ibf_set.add_peer(peer_id.clone(), ibf_set, &mut self.edges_info);
                RoutingTableMessagesResponse::AddPeerResponse { seed }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::RemovePeer(peer_id) => {
                self.peer_ibf_set.remove_peer(&peer_id);
                RoutingTableMessagesResponse::Empty
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::ProcessIbfMessage { peer_id, ibf_msg } => {
                match ibf_msg.routing_state {
                    RoutingState::PartialSync(partial_sync) => {
                        if let Some(ibf_set) = self.peer_ibf_set.get(&peer_id) {
                            let seed = ibf_msg.seed;
                            let (edges_for_peer, unknown_edge_hashes, unknown_edges_count) = self
                                .exchange_routing_tables_using_ibf(
                                    &peer_id,
                                    ibf_set,
                                    partial_sync.ibf_level,
                                    &partial_sync.ibf,
                                    ibf_msg.seed,
                                );

                            let edges_for_peer = edges_for_peer
                                .iter()
                                .filter_map(|x| self.edges_info.get(&x.key()).cloned())
                                .collect();
                            // Prepare message
                            let ibf_msg = if unknown_edges_count == 0
                                && unknown_edge_hashes.len() > 0
                            {
                                RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: RoutingState::RequestMissingEdges(
                                        unknown_edge_hashes,
                                    ),
                                }
                            } else if unknown_edges_count == 0 && unknown_edge_hashes.len() == 0 {
                                RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: RoutingState::Done,
                                }
                            } else {
                                if let Some(new_ibf_level) = partial_sync.ibf_level.inc() {
                                    let ibf_vec = ibf_set.get_ibf_vec(new_ibf_level);
                                    RoutingVersion2 {
                                        known_edges: self.edges_info.len() as u64,
                                        seed,
                                        edges: edges_for_peer,
                                        routing_state: RoutingState::PartialSync(PartialSync {
                                            ibf_level: new_ibf_level,
                                            ibf: ibf_vec,
                                        }),
                                    }
                                } else {
                                    RoutingVersion2 {
                                        known_edges: self.edges_info.len() as u64,
                                        seed,
                                        edges: self
                                            .edges_info
                                            .iter()
                                            .map(|x| x.1.clone())
                                            .collect(),
                                        routing_state: RoutingState::RequestAllEdges,
                                    }
                                }
                            };
                            RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                                ibf_msg: Some(ibf_msg),
                            }
                        } else {
                            error!(target: "network", "Peer not found {}", peer_id);
                            RoutingTableMessagesResponse::Empty
                        }
                    }
                    RoutingState::InitializeIbf => {
                        self.peer_ibf_set.add_peer(
                            peer_id.clone(),
                            Some(ibf_msg.seed),
                            &mut self.edges_info,
                        );
                        if let Some(ibf_set) = self.peer_ibf_set.get(&peer_id) {
                            let seed = ibf_set.get_seed();
                            let ibf_vec = ibf_set.get_ibf_vec(MIN_IBF_LEVEL);
                            RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                                ibf_msg: Some(RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: Default::default(),
                                    routing_state: RoutingState::PartialSync(PartialSync {
                                        ibf_level: MIN_IBF_LEVEL,
                                        ibf: ibf_vec,
                                    }),
                                }),
                            }
                        } else {
                            error!(target: "network", "Peer not found {}", peer_id);
                            RoutingTableMessagesResponse::Empty
                        }
                    }
                    RoutingState::RequestMissingEdges(requested_edges) => {
                        let seed = ibf_msg.seed;
                        let (edges_for_peer, _) =
                            self.split_edges_for_peer(&peer_id, &requested_edges);

                        let edges_for_peer = edges_for_peer
                            .iter()
                            .filter_map(|x| self.edges_info.get(&x.key()).cloned())
                            .collect();

                        let ibf_msg = RoutingVersion2 {
                            known_edges: self.edges_info.len() as u64,
                            seed,
                            edges: edges_for_peer,
                            routing_state: RoutingState::Done,
                        };
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: Some(ibf_msg),
                        }
                    }
                    RoutingState::RequestAllEdges => {
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: Some(RoutingVersion2 {
                                known_edges: self.edges_info.len() as u64,
                                seed: ibf_msg.seed,
                                edges: self.edges_info.iter().map(|x| x.1.clone()).collect(),
                                routing_state: RoutingState::Done,
                            }),
                        }
                    }
                    RoutingState::Done => {
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse { ibf_msg: None }
                    }
                }
            }
        }
    }
}

pub fn make_routing_table_actor(peer_id: PeerId, store: Arc<Store>) -> Addr<RoutingTableActor> {
    RoutingTableActor::new(peer_id, store).start()
}
