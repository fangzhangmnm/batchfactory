from .op_graph import OpGraphEdge, OpGraph
from .op_base import *
from typing import List, Dict, Tuple
from ..lib.utils import _number_to_label

class OpGraphSegment:
    def __init__(self):
        self.nodes:List[BaseOp] = []
        self.edges:List[OpGraphEdge] = []
        self.head:BaseOp = None
        self.tail:BaseOp = None
    @classmethod
    def make_seg(cls,seg:'OpGraphSegment|BaseOp')->'OpGraphSegment':
        if isinstance(seg, OpGraphSegment): return seg
        elif isinstance(seg, BaseOp):
            node,seg= seg, cls()
            seg.nodes.append(node)
            seg.head = node
            seg.tail = node
            return seg
        else: raise TypeError(f"Cannot make OpGraphSegment from {type(seg)}")
    def __or__(self,other:'OpGraphSegment|BaseOp')->'OpGraphSegment':
        other=OpGraphSegment.make_seg(other)
        if not other.head: raise ValueError(f"Segment {other} has no head node.")
        if not self.tail: raise ValueError(f"Segment {self} has no tail node.")
        if set(self.nodes) & set(other.nodes): raise ValueError(f"Segments {self} and {other} have overlapping nodes.")
        if not self.is_out_port_abaliable(self.tail, 0): raise ValueError(f"Port 0 of tail node {self.tail} is already used.")
        if not other.is_in_port_abaliable(other.head, 0): raise ValueError(f"Port 0 of head node {other.head} is already used.")
        self.edges.append(OpGraphEdge(self.tail, other.head, 0, 0))
        self.nodes.extend(other.nodes) 
        self.edges.extend(other.edges)
        self.tail = other.tail
        return self
    def __repr__(self):
        return _repr_graph("OpGraphSegment()",self.nodes, self.edges)
    def compile(self)->'OpGraph':
        return OpGraph(self.nodes, self.edges)
    def is_in_port_abaliable(self,node:BaseOp,port:int)->bool:
        return not any(e for e in self.edges if e.target == node and e.target_port == port)
    def is_out_port_abaliable(self,node:BaseOp,port:int)->bool:
        return not any(e for e in self.edges if e.source == node and e.source_port == port)
    
    def wire(self,source,targer,source_port=0,target_port=0):
        if isinstance(source, OpGraphSegment):
            self.merge(source)
            source = source.tail
        if isinstance(targer, OpGraphSegment):
            self.merge(targer)
            targer = targer.head
        if not source in self.nodes:
            raise ValueError(f"Node {source} is not in the segment.")
        if not targer in self.nodes:
            raise ValueError(f"Node {targer} is not in the segment.")
        if not self.is_in_port_abaliable(targer, target_port):
            raise ValueError(f"Input Port {target_port} of node {targer} is already used.")
        if not self.is_out_port_abaliable(source, source_port):
            raise ValueError(f"Output Port {source_port} of node {source} is already used.")
        self.edges.append(OpGraphEdge(source, targer, source_port, target_port))

    def merge(self,other:'OpGraphSegment|BaseOp')->None:
        other = OpGraphSegment.make_seg(other)
        for node in other.nodes:
            if node not in self.nodes:
                self.nodes.append(node)
        for edge in other.edges:
            if edge not in self.edges:
                self.wire(edge.source, edge.target, edge.source_port, edge.target_port)

        

def _repr_graph(title,nodes,edges,node_info=None):
    if _is_chain(nodes, edges):
        return "|".join(repr(node) for node in nodes)
    else:
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


    

def _is_chain(nodes,edges):
    if len(edges)!= len(nodes) - 1:
        return False
    for i in range(len(nodes) - 1):
        if OpGraphEdge(nodes[i], nodes[i + 1]) not in edges:
            return False
    return True

# def _allow_single_predecessor(node:BaseOp):
#     return not isinstance(node,(InputOp,MergeOp))
# def _allow_single_successor(node:BaseOp):
#     # OutputOp is not terminating, it passes entries to the next node
#     return not isinstance(node, (SplitOp))


__all__ = [
    "OpGraphSegment"
]