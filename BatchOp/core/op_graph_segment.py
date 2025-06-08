from .op_graph import OpEdge, OpGraph
from .op_node import *
from typing import List, Dict, Tuple

class OpGraphSegment:
    def __init__(self):
        self.nodes:List[BaseOp] = []
        self.edges:List[OpEdge] = []
        self.head:BaseOp = None
        self.tail:BaseOp = None
    @classmethod
    def make_seg(cls,seg:'OpGraphSegment|BaseOp')->'OpGraphSegment':
        if isinstance(seg, OpGraphSegment): return seg
        elif isinstance(seg, BaseOp):
            node,seg= seg, cls()
            seg.nodes.append(node)
            if _allow_single_predecessor(node):
                seg.head = node
            if _allow_single_successor(node):
                seg.tail = node
            return seg
        else: raise TypeError(f"Cannot make OpGraphSegment from {type(seg)}")
    def __or__(self,other:'OpGraphSegment|BaseOp')->'OpGraphSegment':
        other=OpGraphSegment.make_seg(other)
        if not other.head: raise ValueError(f"Segment {other} has no head node.")
        if not self.tail: raise ValueError(f"Segment {self} has no tail node.")
        if set(self.nodes) & set(other.nodes): raise ValueError(f"Segments {self} and {other} have overlapping nodes.")
        self.edges.append(OpEdge(self.tail, other.head))
        self.nodes.extend(other.nodes) 
        self.edges.extend(other.edges)
        self.tail = other.tail
        return self
    def __repr__(self):
        return _repr_graph(self.nodes, self.edges)
        
    def compile(self):
        return OpGraph(self.nodes, self.edges)
    # TODO wiring (which might break head and tail def)

def _repr_graph(nodes,edges):
    if _is_chain(nodes, edges):
        return "|".join(repr(node) for node in nodes)
    else:
        return "Graph\n"+repr(nodes) + "\n" + repr(edges)

def _is_chain(nodes,edges):
    for i in range(len(nodes) - 1):
        if OpEdge(nodes[i], nodes[i + 1]) not in edges:
            return False
    return True

def _allow_single_predecessor(node:BaseOp):
    return not isinstance(node,(InputOp,MergeOp))
def _allow_single_successor(node:BaseOp):
    # OutputOp is not terminating, it passes entries to the next node
    return not isinstance(node, (SplitOp))


__all__ = [
    "OpGraphSegment"
]