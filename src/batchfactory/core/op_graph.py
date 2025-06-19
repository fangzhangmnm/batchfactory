
from .base_op import *
from .entry import Entry
from typing import List, Dict, Tuple, TYPE_CHECKING
from ..lib.utils import _number_to_label
from typing import NamedTuple
if TYPE_CHECKING:
    from .executor import OpGraphExecutor

class OpGraphEdge(NamedTuple):
    source: BaseOp
    target: BaseOp
    source_port: int=0
    target_port: int=0

class Graph:
    def __init__(self):
        self.nodes:List[BaseOp] = []
        self.edges:List[OpGraphEdge] = []
        self.head:BaseOp = None
        self.tail:BaseOp = None
        self.executor: 'OpGraphExecutor' = None
    def to_graph(self):
        return self
    def __or__(self,other:'Graph|BaseOp')->'Graph':
        return self.merge(other, chain=True)
    def __getitem__(self, tag:str)->BaseOp:
        return self.get_unique_node_by_tag(tag)
    def merge(self, other:'Graph|BaseOp', chain=False)->'Graph':
        return OpGraphConnector.merge(self, other, chain=chain)
    def wire(self,source:'str|BaseOp|Graph',target,source_port=0,target_port=0)->None:
        OpGraphConnector.wire(self, source, target, source_port, target_port)
    def is_in_port_abaliable(self,node:BaseOp,port:int)->bool:
        return not any(e for e in self.edges if e.target == node and e.target_port == port)
    def is_out_port_abaliable(self,node:BaseOp,port:int)->bool:
        return not any(e for e in self.edges if e.source == node and e.source_port == port)
    def is_chain(self):
        return OpGraphConnector.is_chain(self)
    def get_node_by_tag(self, tag:str)->BaseOp:
        return OpGraphConnector.get_unique_node_by_tag(self, tag)
    def get_output(self, node:BaseOp, port:int=None)->Dict[int,Dict[str,Entry]]|Dict[str,Entry]:
        return self.executor.get_node_output(node, port)
    def get_executor(self):
        if self.executor is None:
            from .executor import OpGraphExecutor
            self.executor = OpGraphExecutor(self)
        return self.executor
    def __repr__(self):
        node_info = None
        if self.executor is not None:
            node_info = self.executor.get_cache_summary()
        return summary_graph("OpGraph()", self, node_info=node_info)
    def execute(self, 
                dispatch_brokers=False, 
                mock=False, 
                max_iterations = 1000, 
                max_barrier_level:int|None = None):
        executor = self.get_executor()
        return executor.execute(
            dispatch_brokers=dispatch_brokers, 
            mock=mock, 
            max_iterations=max_iterations, 
            max_barrier_level=max_barrier_level
        )

def summary_graph(title,graph,node_info=None):
    nodes, edges = graph.nodes, graph.edges
    # if graph.is_chain() and node_info is None:
    #     return "|".join(repr(node) for node in nodes)
    node_label = {node: _number_to_label(idx+1) for idx, node in enumerate(nodes)}
    node_outputs = {node: {} for node in nodes}
    for edge in edges:
        node_outputs[edge.source][edge.source_port] = (edge.target,edge.target_port)
    text=f"{title}\n"
    for node in nodes:
        text += f"(op{node_label[node]}): {repr(node)}"
        text += " -> "
        desc = []
        for source_port in range(max(node_outputs[node].keys(), default=0) + 1):
            if source_port in node_outputs[node]:
                target, target_port = node_outputs[node][source_port]
                if target_port>0:
                    desc.append(f"op{node_label[target]}[{target_port}]")
                else:
                    desc.append(f"op{node_label[target]}")
            else:
                desc.append("None")
        text += ", ".join(desc)
        if node_info and node in node_info:
            text += ": " + node_info[node]
        text += "\n"
    return text

class OpGraphConnector:
    @staticmethod
    def merge(self,other:'Graph|BaseOp',chain=False)->None:
        other = OpGraphConnector.make_graph(other)
        if len(other.nodes) == 0: raise ValueError(f"Should not merge an empty OpGraph {other}.")
        if len(self.nodes) == 0:
            self.nodes = other.nodes.copy()
            self.edges = other.edges.copy()
            self.head = other.head
            self.tail = other.tail
            self.executor = other.executor
            return self
        else:
            if self.executor is not None and other.executor is not None: raise ValueError("Cannot merge two OpGraphs with executors.")
            if set(self.nodes) & set(other.nodes): raise ValueError(f"Segments {self} and {other} have overlapping nodes.")
            if chain:
                if self.tail is None: raise ValueError(f"Segment {self} has no tail node.")
                if not self.is_out_port_abaliable(self.tail, 0): raise ValueError(f"Port 0 of tail node {self.tail} is already used.")
                if other.head is None: raise ValueError(f"Segment {other} has no head node.")
                if not other.is_in_port_abaliable(other.head, 0): raise ValueError(f"Port 0 of head node {other.head} is already used.")
                self.edges.append(OpGraphEdge(self.tail, other.head, 0, 0))
                self.tail = other.tail
            self.nodes.extend(other.nodes)
            self.edges.extend(other.edges)
            if self.executor is None and other.executor is not None:
                self.executor = other.executor
            other.executor = None
            return self
    @staticmethod
    def wire(graph,source,target,source_port=0,target_port=0):
        ### Check Rejection before doing modifications
        def detect_relationship(other:Graph)->str:
            intersection_size = len(set(graph.nodes) & set(other.nodes))
            if intersection_size == 0: return "disjoint"
            elif intersection_size == len(other.nodes): return "subgraph"
            else: return "illegal"
        if isinstance(source, str):
            source = graph.get_unique_node_by_tag(source)
            if source is None:
                raise ValueError(f"Node with tag {source} not found in the current graph.")
        if isinstance(target, str):
            target = graph.get_unique_node_by_tag(target)
            if target is None:
                raise ValueError(f"Node with tag {target} not found in the current graph.")
        contains_source = source in graph.nodes
        contains_target = target in graph.nodes
        if not contains_source and not contains_target:
            raise ValueError("At least one of source or target must be a node in the current graph.")
        if contains_source and not graph.is_out_port_abaliable(source, source_port):
            raise ValueError(f"Output Port {source_port} of node {source} is already used.")
        if contains_target and not graph.is_in_port_abaliable(target, target_port):
            raise ValueError(f"Input Port {target_port} of node {target} is already used.")
        if isinstance(source, Graph):
            source_relationship = detect_relationship(source)
            if source_relationship == "illegal":
                raise ValueError(f"Cannot wire a graph segment {source} that is not a subgraph or disjoint from the current graph {graph}.")
            if source.tail is None:
                raise ValueError(f"Segment {source} has no tail node.")
            if not source.is_out_port_abaliable(source.tail, source_port):
                raise ValueError(f"Output Port {source_port} of tail node {source.tail} is already used.")
        if isinstance(target, Graph):
            target_relationship = detect_relationship(target)
            if target_relationship == "illegal":
                raise ValueError(f"Cannot wire a graph segment {target} that is not a subgraph or disjoint from the current graph {graph}.")
            if target.head is None:
                raise ValueError(f"Segment {target} has no head node.")
            if not target.is_in_port_abaliable(target.head, target_port):
                raise ValueError(f"Input Port {target_port} of head node {target.head} is already used.")
        ### End of Rejection Check
        if isinstance(source, Graph):
            if source_relationship == "disjoint":
                graph.merge(source)
            source = source.tail
        if isinstance(target, Graph):
            if target_relationship == "disjoint":
                graph.merge(target)
            target = target.head
        graph.edges.append(OpGraphEdge(source, target, source_port, target_port))
    @staticmethod
    def make_graph(source:Graph|BaseOp)->Graph:
        if isinstance(source, Graph): 
            return source
        elif isinstance(source, BaseOp):
            node, graph= source, Graph()
            graph.nodes.append(node)
            graph.head = node
            graph.tail = node
            return graph
        else: 
            raise TypeError(f"Cannot make OpGraphSegment from {type(source)}")
    @staticmethod
    def is_chain(graph):
        if len(graph.edges)!= len(graph.nodes) - 1:
            return False
        for i in range(len(graph.nodes) - 1):
            if OpGraphEdge(graph.nodes[i], graph.nodes[i + 1]) not in graph.edges:
                return False
        return True
    @staticmethod
    def get_unique_node_by_tag(graph:Graph, tag:str)->BaseOp:
        nodes = [node for node in graph.nodes if node._tag == tag]
        # return nodes[0] if len(nodes) == 1 else None
        if len(nodes)==1:
            return nodes[0]
        elif len(nodes) == 0:
            raise KeyError(f"Node with tag {tag} not found in the current graph.")
        else:   
            raise KeyError(f"Multiple nodes with tag {tag} found in the current graph: {nodes}. Please use a unique tag for each node.")



__all__ = [
    "Graph"
]