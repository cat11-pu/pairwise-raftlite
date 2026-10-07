"""A miniature Raft core: leader election and log replication.

The module is self contained on purpose.  Nodes exchange plain dataclass
messages through an in-process queue and read the time from an injected clock,
so a whole cluster can be simulated deterministically without sockets, threads
or any third party package.

Vocabulary used throughout:

* ``term``            monotonically increasing election number
* ``index``           1-based position of an entry in the log
* ``commit_index``    highest log index known to be committed
"""

from __future__ import annotations

import random


# --------------------------------------------------------------------------- messages


class LogEntry:
    """One replicated command, tagged with the term of the leader that made it."""

    __slots__ = ("term", "command")

    def __init__(self, term, command):
        self.term = term
        self.command = command

    def __eq__(self, other):
        if not isinstance(other, LogEntry):
            return NotImplemented
        return self.term == other.term and self.command == other.command

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self):
        return hash((self.term, self.command))

    def __repr__(self):
        return "LogEntry(term=%r, command=%r)" % (self.term, self.command)


class RequestVote:
    """A candidate asking a peer for its vote in ``term``."""

    __slots__ = ("term", "candidate", "last_log_index", "last_log_term")

    def __init__(self, term, candidate, last_log_index, last_log_term):
        self.term = term
        self.candidate = candidate
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def __repr__(self):
        return "RequestVote(term=%r, candidate=%r, last_log_index=%r, last_log_term=%r)" % (
            self.term,
            self.candidate,
            self.last_log_index,
            self.last_log_term,
        )


class RequestVoteReply:
    """The answer to a RequestVote; ``peer`` is whoever answered."""

    __slots__ = ("term", "peer", "granted")

    def __init__(self, term, peer, granted):
        self.term = term
        self.peer = peer
        self.granted = granted

    def __repr__(self):
        return "RequestVoteReply(term=%r, peer=%r, granted=%r)" % (
            self.term,
            self.peer,
            self.granted,
        )


class AppendEntries:
    """A leader replicating entries and/or sending a heartbeat."""

    __slots__ = ("term", "leader", "prev_log_index", "prev_log_term", "entries", "leader_commit")

    def __init__(self, term, leader, prev_log_index, prev_log_term, entries, leader_commit):
        self.term = term
        self.leader = leader
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = list(entries)
        self.leader_commit = leader_commit

    def __repr__(self):
        return "AppendEntries(term=%r, leader=%r, prev_log_index=%r, prev_log_term=%r, entries=%r, leader_commit=%r)" % (
            self.term,
            self.leader,
            self.prev_log_index,
            self.prev_log_term,
            self.entries,
            self.leader_commit,
        )


class AppendEntriesReply:
    """The answer to an AppendEntries; ``matched_index`` is the log length after the call."""

    __slots__ = ("term", "peer", "success", "matched_index")

    def __init__(self, term, peer, success, matched_index):
        self.term = term
        self.peer = peer
        self.success = success
        self.matched_index = matched_index

    def __repr__(self):
        return "AppendEntriesReply(term=%r, peer=%r, success=%r, matched_index=%r)" % (
            self.term,
            self.peer,
            self.success,
            self.matched_index,
        )


# --------------------------------------------------------------------------- clock


class Clock:
    """A manual clock; the caller decides how fast simulated time moves."""

    def __init__(self, now=0.0):
        self._now = float(now)

    def now(self):
        return self._now

    def advance(self, seconds):
        self._now += float(seconds)
        return self._now

    def __repr__(self):
        return "Clock(now=%r)" % (self._now,)


# --------------------------------------------------------------------------- storage


class Storage:
    """The durable part of a node's state: term, vote, log and commit index."""

    def __init__(self):
        self.term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0

    def save(self, term, voted_for, log, commit_index):
        self.term = term
        self.voted_for = voted_for
        self.log = list(log)
        self.commit_index = commit_index

    def load(self):
        return self.term, self.voted_for, list(self.log), self.commit_index

    def __repr__(self):
        return "Storage(term=%r, voted_for=%r, log=%r, commit_index=%r)" % (
            self.term,
            self.voted_for,
            self.log,
            self.commit_index,
        )


# --------------------------------------------------------------------------- network


class Network:
    """In-process message queue connecting a set of nodes."""

    def __init__(self, clock=None):
        self.clock = clock if clock is not None else Clock()
        self.nodes = {}
        self.pending = []
        self.blocked = set()
        self.delivered = 0

    def add_node(self, node_id, **kwargs):
        node = RaftNode(node_id, self, **kwargs)
        self.nodes[node_id] = node
        return node

    def peers_of(self, node_id):
        return [other for other in self.nodes if other != node_id]

    def block(self, node_id):
        """Silently drop every message to and from ``node_id``."""
        self.blocked.add(node_id)

    def unblock(self, node_id):
        self.blocked.discard(node_id)

    def send(self, dst, src, message):
        if dst in self.blocked or src in self.blocked:
            return False
        self.pending.append((dst, src, message))
        return True

    def settle(self, limit=500):
        """Deliver queued messages until the cluster stops talking."""
        rounds = 0
        while self.pending and rounds < limit:
            batch, self.pending = self.pending, []
            for dst, src, message in batch:
                self.delivered += 1
                self.nodes[dst].receive(src, message)
            rounds += 1
        if self.pending:
            raise RuntimeError("message storm: %d undelivered" % len(self.pending))
        return rounds


# --------------------------------------------------------------------------- node


class RaftNode:
    """One cluster member: election state machine plus log replication."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

    def __init__(
        self,
        node_id,
        network,
        storage=None,
        seed=None,
        election_timeout=500.0,
        heartbeat_interval=100.0,
        jitter=0.2,
    ):
        self.id = node_id
        self.network = network
        self.clock = network.clock
        self.store = storage if storage is not None else Storage()
        self.rng = random.Random(seed if seed is not None else node_id)
        self.election_timeout = float(election_timeout)
        self.heartbeat_interval = float(heartbeat_interval)
        self.jitter = float(jitter)

        term, voted_for, log, commit_index = self.store.load()
        self.current_term = term
        self.voted_for = voted_for
        self.log = list(log)
        self.commit_index = commit_index

        self.state = self.FOLLOWER
        self.leader_id = None
        self.votes_received = set()
        self.next_index = {}
        self.match_index = {}
        self.applied = []
        self.election_deadline = 0.0
        self.heartbeat_deadline = 0.0
        self._reset_election_timer()

    def __repr__(self):
        return "<RaftNode %s term=%r state=%s log=%d commit=%r>" % (
            self.id,
            self.current_term,
            self.state,
            len(self.log),
            self.commit_index,
        )

    # ------------------------------------------------------------------ durable state

    def persist(self):
        """Copy the durable part of our state to storage."""
        self.store.save(self.current_term, self.voted_for, self.log, self.commit_index)

    def restart(self):
        """Simulate a crash and reboot this node from its storage."""
        term, voted_for, log, commit_index = self.store.load()
        self.current_term = term
        self.voted_for = voted_for
        self.log = list(log)
        self.commit_index = commit_index
        self.state = self.FOLLOWER
        self.leader_id = None
        self.votes_received = set()
        self.next_index = {}
        self.match_index = {}
        self.applied = []
        self._reset_election_timer()

    # ------------------------------------------------------------------ log helpers

    def last_log_index(self):
        return len(self.log)

    def last_log_term(self):
        if not self.log:
            return 0
        return self.log[-1].term

    def entry_at(self, index):
        """The entry stored at the 1-based ``index``, or ``None``."""
        if index < 1 or index > len(self.log):
            return None
        return self.log[index - 1]

    def entry_term(self, index):
        entry = self.entry_at(index)
        return 0 if entry is None else entry.term

    def append_entry(self, command):
        """Client request handled by a leader: append and start replicating."""
        if self.state != self.LEADER:
            return None
        self.log.append(LogEntry(self.current_term, command))
        self.match_index[self.id] = len(self.log)
        self.persist()
        self._broadcast_heartbeat()
        return len(self.log)

    # ------------------------------------------------------------------ elections

    def _reset_election_timer(self):
        span = self.election_timeout * (1.0 + self.rng.uniform(0.0, self.jitter))
        self.election_deadline = self.clock.now() + span

    def _quorum_size(self):
        peers = len(self.network.peers_of(self.id))
        return (peers + 1) // 2 + 1

    def start_election(self):
        """Move to a new term, vote for ourselves and ask everyone for a vote."""
        self.state = self.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes_received = {self.id}
        self.leader_id = None
        self.persist()
        self._reset_election_timer()
        self._broadcast(
            RequestVote(self.current_term, self.id, self.last_log_index(), self.last_log_term())
        )
        return self.current_term

    def handle_request_vote(self, msg):
        if msg.term < self.current_term:
            return RequestVoteReply(self.current_term, self.id, False)
        if msg.term > self.current_term:
            self._become_follower(msg.term)
        if self.voted_for is not None and self.voted_for != msg.candidate:
            return RequestVoteReply(self.current_term, self.id, False)
        if not self._candidate_log_is_up_to_date(msg.last_log_index, msg.last_log_term):
            return RequestVoteReply(self.current_term, self.id, False)
        self.state = self.FOLLOWER
        self.leader_id = None
        self.voted_for = msg.candidate
        self._reset_election_timer()
        self.persist()
        return RequestVoteReply(self.current_term, self.id, True)

    def _candidate_log_is_up_to_date(self, last_log_index, last_log_term):
        my_term = self.last_log_term()
        if last_log_term != my_term:
            return last_log_term > my_term
        return last_log_index >= self.last_log_index()

    def handle_request_vote_reply(self, msg):
        if msg.term > self.current_term:
            self._become_follower(msg.term)
            return
        if self.state != self.CANDIDATE or not msg.granted:
            return
        self.votes_received.add(msg.peer)
        if len(self.votes_received) >= self._quorum_size():
            self._become_leader()

    def _become_leader(self):
        self.state = self.LEADER
        self.leader_id = self.id
        self.votes_received = set()
        peers = self.network.peers_of(self.id)
        self.next_index = {peer: self.last_log_index() + 1 for peer in peers}
        self.match_index = {peer: 0 for peer in peers}
        self.match_index[self.id] = self.last_log_index()
        self.heartbeat_deadline = self.clock.now()
        self._broadcast_heartbeat()

    def _become_follower(self, term):
        self.state = self.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = None
        self.votes_received = set()
        self.next_index = {}
        self.match_index = {}
        self.persist()
        self._reset_election_timer()

    # ------------------------------------------------------------------ replication

    def _broadcast(self, message):
        for peer in self.network.peers_of(self.id):
            self.network.send(peer, self.id, message)

    def _broadcast_heartbeat(self):
        for peer in self.network.peers_of(self.id):
            self._send_append_entries(peer)

    def _send_append_entries(self, peer):
        next_index = self.next_index.get(peer, self.last_log_index() + 1)
        prev_index = next_index - 1
        message = AppendEntries(
            self.current_term,
            self.id,
            prev_index,
            self.entry_term(prev_index),
            self.log[prev_index:],
            self.commit_index,
        )
        return self.network.send(peer, self.id, message)

    def handle_append_entries(self, msg):
        if msg.term < self.current_term:
            return AppendEntriesReply(self.current_term, self.id, False, self.last_log_index())
        if msg.term > self.current_term:
            self.current_term = msg.term
            self.voted_for = None
            self.persist()
        self.state = self.FOLLOWER
        self.leader_id = msg.leader
        self._reset_election_timer()
        if msg.prev_log_index > self.last_log_index():
            return AppendEntriesReply(self.current_term, self.id, False, self.last_log_index())
        if self.entry_term(msg.prev_log_index) != msg.prev_log_term:
            return AppendEntriesReply(self.current_term, self.id, False, self.last_log_index())
        self._merge_entries(msg.prev_log_index, msg.entries)
        self._advance_follower_commit(msg.leader_commit)
        return AppendEntriesReply(self.current_term, self.id, True, self.last_log_index())

    def _merge_entries(self, prev_index, entries):
        """Install the leader's entries after ``prev_index``."""
        for offset, entry in enumerate(entries):
            index = prev_index + offset + 1
            stored = self.entry_at(index)
            if stored is None:
                self.log.append(entry)
            elif stored.term != entry.term:
                self._truncate_from(index)
                self.log.append(entry)
        self.persist()

    def _truncate_from(self, index):
        """Edit the stored log at ``index``."""
        del self.log[index - 1 :]

    def _advance_follower_commit(self, leader_commit):
        target = min(leader_commit, self.last_log_index())
        if target > self.commit_index:
            for index in range(self.commit_index + 1, target + 1):
                self.applied.append(self.entry_at(index))
            self.commit_index = target
            self.persist()

    def _majority_match_index(self):
        """The match index the leader uses when it advances the commit index."""
        matches = sorted(self.match_index.values(), reverse=True)
        return matches[len(matches) // 2]

    def _advance_commit(self):
        if self.state != self.LEADER:
            return
        index = self._majority_match_index()
        if index > self.commit_index and self.entry_term(index) == self.current_term:
            self.commit_index = index
            self.persist()

    def handle_append_entries_reply(self, msg):
        if msg.term > self.current_term:
            self._become_follower(msg.term)
            return
        if self.state != self.LEADER:
            return
        if msg.term < self.current_term:
            return
        if msg.success:
            matched = min(msg.matched_index, self.last_log_index())
            self.match_index[msg.peer] = matched
            self.next_index[msg.peer] = matched + 1
            if self.next_index[msg.peer] <= self.last_log_index():
                self._send_append_entries(msg.peer)
        elif self.next_index.get(msg.peer, 1) > 1:
            self.next_index[msg.peer] = self.next_index[msg.peer] - 1
            self._send_append_entries(msg.peer)
        self._advance_commit()

    # ------------------------------------------------------------------ timers

    def tick(self):
        """Let simulated time pass for this node."""
        now = self.clock.now()
        if self.state == self.LEADER:
            if now >= self.heartbeat_deadline:
                self.heartbeat_deadline = now + self.heartbeat_interval
                self._broadcast_heartbeat()
            return
        if now >= self.election_deadline:
            self.start_election()

    # ------------------------------------------------------------------ message entry

    def receive(self, sender, message):
        if isinstance(message, RequestVote):
            reply = self.handle_request_vote(message)
        elif isinstance(message, RequestVoteReply):
            reply = self.handle_request_vote_reply(message)
        elif isinstance(message, AppendEntries):
            reply = self.handle_append_entries(message)
        elif isinstance(message, AppendEntriesReply):
            reply = self.handle_append_entries_reply(message)
        else:
            raise TypeError("unsupported message: %r" % (message,))
        if reply is not None:
            self.network.send(sender, self.id, reply)
        return reply
