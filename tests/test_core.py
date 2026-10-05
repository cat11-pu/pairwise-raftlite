"""Behaviour tests for the raftlite core."""

import unittest

from raftlite.core import (
    AppendEntries,
    AppendEntriesReply,
    Clock,
    LogEntry,
    Network,
    RaftNode,
    RequestVote,
)

NODE_IDS = ("A", "B", "C", "D", "E")


def build_cluster(clock, count=3, election_timeout=100.0, heartbeat_interval=30.0):
    """A cluster whose members time out one after another, so runs are reproducible."""
    net = Network(clock)
    nodes = [
        net.add_node(
            node_id,
            election_timeout=election_timeout * (index + 1),
            heartbeat_interval=heartbeat_interval,
            jitter=0.0,
        )
        for index, node_id in enumerate(NODE_IDS[:count])
    ]
    return net, nodes


def leaders_in(net):
    return [node for node in net.nodes.values() if node.state == RaftNode.LEADER]


def run_until(predicate, net, clock, step=10.0, limit=200):
    for _ in range(limit):
        clock.advance(step)
        for node in list(net.nodes.values()):
            node.tick()
        net.settle()
        if predicate():
            return True
    return False


def commands(node):
    return [entry.command for entry in node.log]


class ElectionTests(unittest.TestCase):
    def test_cluster_elects_a_leader_replicates_and_survives_failover(self):
        clock = Clock()
        net, (a, b, c) = build_cluster(clock)

        self.assertTrue(run_until(lambda: len(leaders_in(net)) == 1, net, clock))
        self.assertEqual([node.id for node in leaders_in(net)], ["A"])
        for node in (a, b, c):
            self.assertEqual(node.current_term, 1)
            self.assertEqual(node.leader_id, "A")

        index = a.append_entry("set x 1")
        self.assertEqual(index, 1)
        net.settle()
        for node in (a, b, c):
            self.assertEqual(commands(node), ["set x 1"])
        self.assertEqual(a.commit_index, 1)
        self.assertTrue(run_until(lambda: c.commit_index == 1, net, clock))

        # A goes away: the two survivors have to elect a new leader and keep
        # the entry that is already committed.
        net.block("A")
        self.assertTrue(run_until(lambda: b.state == RaftNode.LEADER, net, clock))
        self.assertEqual(b.current_term, 2)
        self.assertEqual(c.current_term, 2)
        self.assertEqual(c.leader_id, "B")
        self.assertEqual(commands(b), ["set x 1"])
        self.assertEqual(commands(c), ["set x 1"])

        # The new leader keeps serving clients.
        self.assertEqual(b.append_entry("set y 2"), 2)
        net.settle()
        self.assertEqual(b.commit_index, 2)
        self.assertTrue(run_until(lambda: c.commit_index == 2, net, clock))
        self.assertEqual(commands(c), ["set x 1", "set y 2"])

    def test_candidate_term_and_self_vote_survive_a_restart(self):
        clock = Clock()
        net = Network(clock)
        node = net.add_node("A", election_timeout=100.0, jitter=0.0)
        net.add_node("B", election_timeout=200.0, jitter=0.0)

        node.start_election()
        self.assertEqual(node.current_term, 1)
        self.assertEqual(node.voted_for, "A")

        node.restart()
        self.assertEqual(node.current_term, 1)
        self.assertEqual(node.voted_for, "A")

        reply = node.handle_request_vote(RequestVote(1, "B", 0, 0))
        self.assertFalse(reply.granted)

    def test_voter_adopts_a_higher_term_before_voting(self):
        clock = Clock()
        net = Network(clock)
        node = net.add_node("B", election_timeout=100.0, jitter=0.0)
        node.current_term = 5
        node.voted_for = "C"
        node.persist()

        reply = node.handle_request_vote(RequestVote(9, "A", 3, 8))

        self.assertTrue(reply.granted)
        self.assertEqual(reply.term, 9)
        self.assertEqual(node.current_term, 9)
        self.assertEqual(node.voted_for, "A")
        self.assertEqual(node.store.term, 9)

    def test_stale_append_entries_does_not_postpone_an_election(self):
        clock = Clock()
        net = Network(clock)
        node = net.add_node("B", election_timeout=100.0, jitter=0.0)
        node.current_term = 5
        node.persist()
        deadline = node.election_deadline

        clock.advance(10.0)
        stale = node.handle_append_entries(AppendEntries(3, "A", 0, 0, [], 0))
        self.assertFalse(stale.success)
        self.assertEqual(node.election_deadline, deadline)

        clock.advance(10.0)
        fresh = node.handle_append_entries(AppendEntries(5, "C", 0, 0, [], 0))
        self.assertTrue(fresh.success)
        self.assertEqual(node.election_deadline, 120.0)

        clock.advance(105.0)
        node.tick()
        self.assertEqual(node.state, RaftNode.CANDIDATE)
        self.assertEqual(node.current_term, 6)

    def test_leader_steps_down_when_a_peer_reports_a_higher_term(self):
        clock = Clock()
        net, (a, b, c) = build_cluster(clock)
        self.assertTrue(run_until(lambda: a.state == RaftNode.LEADER, net, clock))
        self.assertEqual(a.current_term, 1)

        b.current_term = 2
        b.voted_for = None
        b.persist()

        a.heartbeat_deadline = clock.now()
        a.tick()
        net.settle()

        self.assertEqual(a.current_term, 2)
        self.assertEqual(a.state, RaftNode.FOLLOWER)


class ReplicationTests(unittest.TestCase):
    def test_follower_rejects_append_entries_that_do_not_match_its_log(self):
        clock = Clock()
        net = Network(clock)
        follower = net.add_node("B", election_timeout=100.0, jitter=0.0)
        follower.current_term = 2
        follower.log = [LogEntry(1, "keep")]
        follower.persist()

        reply = follower.handle_append_entries(
            AppendEntries(
                term=2,
                leader="A",
                prev_log_index=1,
                prev_log_term=2,
                entries=[LogEntry(2, "extra")],
                leader_commit=1,
            )
        )

        self.assertFalse(reply.success)
        self.assertEqual(commands(follower), ["keep"])
        self.assertEqual(follower.commit_index, 0)

    def test_follower_replaces_conflicting_tail_with_leader_log(self):
        clock = Clock()
        net, (a, b, c) = build_cluster(clock)

        b.log = [LogEntry(1, "stale-1"), LogEntry(1, "stale-2")]
        a.current_term = 4
        a.log = [LogEntry(4, "fresh")]
        a.persist()

        self.assertTrue(run_until(lambda: a.state == RaftNode.LEADER, net, clock))
        self.assertEqual(a.current_term, 5)

        self.assertEqual(commands(b), ["fresh"])
        self.assertEqual(commands(c), ["fresh"])
        self.assertEqual(b.last_log_index(), a.last_log_index())


class CommitTests(unittest.TestCase):
    def test_entry_commits_with_two_of_three_nodes_reachable(self):
        clock = Clock()
        net, (a, b, c) = build_cluster(clock)
        self.assertTrue(run_until(lambda: a.state == RaftNode.LEADER, net, clock))

        net.block("C")
        a.append_entry("set x 1")
        net.settle()
        self.assertEqual(commands(b), ["set x 1"])
        self.assertEqual(a.commit_index, 1)

        self.assertTrue(run_until(lambda: b.commit_index == 1, net, clock))
        self.assertEqual(b.commit_index, 1)

    def test_leader_commits_quorum_entries_but_only_from_its_own_term(self):
        clock = Clock()
        net, nodes = build_cluster(clock, count=5)
        leader = nodes[0]
        leader.state = RaftNode.LEADER
        leader.current_term = 6
        leader.log = [LogEntry(1, "stale-1"), LogEntry(1, "stale-2")]
        leader.match_index = {"A": 2, "B": 2, "C": 2, "D": 0, "E": 0}
        leader.commit_index = 0

        leader.handle_append_entries_reply(AppendEntriesReply(6, "B", True, 2))
        self.assertEqual(leader.commit_index, 0)

        leader.log.append(LogEntry(6, "fresh"))
        leader.match_index = {"A": 3, "B": 3, "C": 3, "D": 0, "E": 0}
        leader.handle_append_entries_reply(AppendEntriesReply(6, "B", True, 3))
        self.assertEqual(leader.commit_index, 3)


if __name__ == "__main__":
    unittest.main()
