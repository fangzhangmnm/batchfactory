from .op_node import AtomicOp, BaseOp, MergeOp, SplitOp, InputOp, OutputOp
from .broker_op import BrokerOp
from .entry import Entry
from ..lib.utils import _number_dict_to_list
from typing import List, Tuple, NamedTuple, Dict
from copy import deepcopy

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

    def _pump_node(self,node,dispatch_broker:bool=False,reset_input=False):
        # Please Ensure DeepCopy
        outputs={}
        inputs = self._collect_node_inputs(node, use_deepcopy=True)
        if isinstance(node, AtomicOp):
            outputs[0]=node.update_batch(inputs[0])
        elif isinstance(node, MergeOp):
            outputs[0] = node.merge_batch(inputs)
        elif isinstance(node, SplitOp):
            outputs = node.route_batch(inputs[0])
        elif isinstance(node, InputOp):
            if reset_input or not node.fire_once:
                outputs[0] = node.generate_batch()
        elif isinstance(node, OutputOp):
            node.output_batch(inputs[0])
            outputs[0] = inputs[0]  # OutputOp does not modify entries, just outputs them
        elif isinstance(node, BrokerOp):
            node.enqueue(inputs[0])
            if dispatch_broker:
                node.dispatch_broker()
            outputs[0] = node.get_results()
        else:
            raise NotImplementedError(f"Operation {node} is not implemented.")
        for port, output in outputs.items():
            self._merge_node_output(node, port, output)
        self._consume_node_input(node)

    def incoming_edges(self,node)->List[OpEdge]:
        return [edge for edge in self.edges if edge.target == node]
    def outgoing_edges(self,node)->List[OpEdge]:
        return [edge for edge in self.edges if edge.source == node]

    def _collect_node_inputs(self,node,use_deepcopy:bool)->List[List[Entry]]:
        inputs={}
        for edge in self.incoming_edges(node):
            port_inputs= list(self.output_cache.setdefault((edge.source, edge.source_port), {}).values())
            if use_deepcopy:
                port_inputs = [deepcopy(entry) for entry in port_inputs]
            inputs.setdefault(edge.target_port, []).extend(port_inputs)
        return _number_dict_to_list(inputs,default_value=[])
        
    def _consume_node_input(self,node):
        """drop entries from cache of incoming nodes if the entry with same idx is in node's output"""
        idx_to_drop=set()
        for edge in self.outgoing_edges(node):
            idx_to_drop.update(self.output_cache.setdefault((node, edge.target_port), {}).keys())
        for edge in self.incoming_edges(node):
            old_results = self.output_cache.setdefault((edge.source, edge.source_port), {})
            for idx in idx_to_drop:
                if idx in old_results:
                    del old_results[idx]
        

    def _merge_node_output(self,node,port,new_results:List[Entry]):
        """merge by idx, newer rev overrides. if same rev, result overrides"""
        old_results= self.output_cache.setdefault((node, port), {})
        for entry in new_results:
            if entry.idx not in old_results or entry.rev>=old_results[entry.idx].rev:
                old_results[entry.idx] = entry
    def pump(self, dispatch_broker:bool=False, reset_input=False):
        for node in self.nodes:
            self._pump_node(node, dispatch_broker=dispatch_broker, reset_input=reset_input)
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

