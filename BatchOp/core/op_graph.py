from .op_node import AtomicOp, BaseOp, MergeOp, RouterOp, InputOp, OutputOp
from .broker_op import BrokerOp
from .entry import Entry
from ..lib.utils import _number_dict_to_list
from typing import List, Tuple, NamedTuple, Dict
import copy

class OpEdge(NamedTuple):
    source: BaseOp
    target: BaseOp
    source_port: int=0
    target_port: int=0

class OpGraph:
    def __init__(self, nodes:List[BaseOp], edges:List[OpEdge]):
        # execution order is determined by order in the nodes array
        self.nodes = nodes
        self.edges = edges
        self.output_cache:Dict[Tuple[BaseOp,int],Dict[Entry]]={}
    def _collect_inputs(self,node)->List[List[Entry]]:
        inputs={}
        for edge in self.edges:
            if edge.target == node:
                port_inputs= list(self.output_cache.get((edge.source, edge.source_port), {}).values())
                inputs.setdefault(edge.target_port, []).extend(port_inputs)
        return _number_dict_to_list(inputs,default_value=[])
    def _pump_node(self,node,dispatch_broker:bool=False):
        outputs={}
        inputs = self._collect_inputs(node)
        if isinstance(node, AtomicOp):
            outputs[0]=node.update_batch(inputs[0])
        elif isinstance(node, MergeOp):
            outputs[0] = node.merge_batch(inputs)
        elif isinstance(node, RouterOp):
            outputs = node.route_batch(inputs[0])
        elif isinstance(node, InputOp):
            outputs[0] = node.generate_batch()
        elif isinstance(node, OutputOp):
            node.output_batch(inputs[0])
            outputs[0] = copy.deepcopy(inputs[0])  # OutputOp does not modify entries, just outputs them
        elif isinstance(node, BrokerOp):
            node.enqueue(inputs[0])
            if dispatch_broker:
                node.dispatch_broker()
            outputs[0] = node.get_results()
        else:
            raise NotImplementedError(f"Operation {node} is not implemented.")
        for port, output in outputs.items():
            self.output_cache.setdefault((node, port), {}).update({e.idx:e for e in output})
    def pump(self, dispatch_broker:bool=False):
        for node in self.nodes:
            self._pump_node(node, dispatch_broker=dispatch_broker)
    def resume(self):
        for node in self.nodes:
            node.resume()
    def __repr__(self):
        from .op_graph_segment import _repr_graph
        return _repr_graph(self.nodes, self.edges)


__all__ = [
    "OpEdge",
    "OpGraph",
]

