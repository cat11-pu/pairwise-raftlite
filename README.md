# pairwise-raftlite

一个只依赖 Python 标准库的迷你 Raft 内核，用来演示与修复分布式一致性缺陷。

节点之间不通过网络通信：所有 RPC 都是普通对象，投递到内存消息队列里，由调用方决定何时投递；
时间来自一个可注入的时钟，由调用方决定走多快。因此整个集群的选举、日志复制与提交过程
可以在单进程里确定性地重放，不需要 socket、线程或任何第三方包。

## 目录

    raftlite/            内核代码
      __init__.py
      core.py            选举状态机、日志复制、持久化状态、提交推进
    tests/
      __init__.py
      test_core.py       行为测试

## 运行测试

在项目根目录执行：

    python3 -m unittest discover -s tests -v

也可以在项目根目录执行全部测试：

    python3 -m unittest discover -v
