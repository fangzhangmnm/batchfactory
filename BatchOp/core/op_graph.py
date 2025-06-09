from .op_node import AtomicOp, BaseOp, MergeOp, SplitOp, InputOp, OutputOp, BatchOp
from .broker_op import BrokerOp
from .entry import Entry
from ..lib.utils import _number_dict_to_list, _pivot_cascaded_dict, _number_to_label
from typing import List, Tuple, NamedTuple, Dict, Set
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
        self.output_cache:Dict[Tuple[BaseOp,int],Dict[str,Entry]]={}
        self.output_revs:Dict[Tuple[BaseOp,int],Dict[str,int]]={}  # used to reject entry with the same revision emitted twice in the same run
        self._has_update_flag=False 

    def _pump_node(self,node,dispatch_broker:bool=False,reset_input=False):
        inputs:Dict[int,Dict[str,Entry]] = self._collect_node_inputs(node, use_deepcopy=True)
        if isinstance(node, AtomicOp):
            for idx,entry in inputs[0].items():
                new_entry = node.update(entry)
                if new_entry is not None:
                    self._update_node_output(node, 0, idx, new_entry)
                    self._consume_node_input(node, idx)
        elif isinstance(node, MergeOp):
            for idx, package in _pivot_cascaded_dict(inputs).items():
                if not node.allow_missing and not all(port in package for port in range(node.n_inputs)):
                    continue
                new_entry = node.merge(package)
                if new_entry is not None:
                    self._update_node_output(node, 0, idx, new_entry)
                    self._consume_node_input(node, idx)
        elif isinstance(node, SplitOp):
            for idx,entry in inputs[0].items():
                output_entries = node.route(entry)
                for port, routed_entry in output_entries.items():
                    if routed_entry is not None:
                        self._update_node_output(node, port, idx, routed_entry)
                if any(output_entries.values()):
                    self._consume_node_input(node, idx)
        elif isinstance(node, InputOp):
            if node.fire_once and not reset_input:
                return
            new_entries = node.generate_batch()
            for idx, entry in new_entries.items():
                self._update_node_output(node, 0, idx, entry)
        elif isinstance(node, OutputOp):
            if len(inputs[0]) == 0:
                return
            node.output_batch(inputs[0])
            for idx, entry in inputs[0].items():
                self._update_node_output(node, 0, idx, entry)
                self._consume_node_input(node, idx)
        elif isinstance(node, BatchOp):
            if len(inputs[0]) == 0:
                return
            new_entries = node.update_batch(inputs[0])
            for idx, entry in new_entries.items():
                self._update_node_output(node, 0, idx, entry)
                self._consume_node_input(node, idx)
            if node.consume_all_batch:
                self._consume_node_inputs_batch(node)
        elif isinstance(node, BrokerOp):
            node.enqueue(inputs[0])
            if dispatch_broker:
                node.dispatch_broker()
            results = node.get_results()
            for idx, entry in results.items():
                self._update_node_output(node, 0, idx, entry)
                self._consume_node_input(node, idx)
        else:
            raise NotImplementedError(f"Operation {node} is not implemented.")

    def incoming_edges(self,node)->List[OpEdge]:
        return [edge for edge in self.edges if edge.target == node]
    def outgoing_edges(self,node)->List[OpEdge]:
        return [edge for edge in self.edges if edge.source == node]

    def _collect_node_inputs(self,node,use_deepcopy:bool)->Dict[int,Dict[str,Entry]]:
        inputs:Dict[int,Dict[str,Entry]] = {port:{} for port in range(node.n_inputs)}
        for edge in self.incoming_edges(node):
            port_inputs = self.output_cache.setdefault((edge.source, edge.source_port), {})
            for idx, entry in port_inputs.items():
                if use_deepcopy:
                    entry = deepcopy(entry)
                inputs.setdefault(edge.target_port, {})[idx] = entry
        return inputs
    
    def _consume_node_input(self,node,idx):
        for edge in self.incoming_edges(node):
            src_entries = self.output_cache.setdefault((edge.source, edge.source_port), {})
            if idx in src_entries:
                del src_entries[idx]
    def _consume_node_inputs_batch(self,node):
        for edge in self.incoming_edges(node):
            src_entries = self.output_cache.setdefault((edge.source, edge.source_port), {})
            src_entries.clear()
    
    def _update_node_output(self,node,port,idx,entry):
        port_entries = self.output_cache.setdefault((node, port), {})
        port_revs = self.output_revs.setdefault((node, port), {})
        if idx in port_revs and entry.rev <= port_revs[idx]:
            return
        if idx not in port_entries or entry.rev >= port_entries[idx].rev:
            port_entries[idx] = entry
            port_revs[idx] = entry.rev
            self._has_update_flag = True

    def pump(self, dispatch_broker:bool=False, reset_input=False)->bool:
        """ Pump the graph, processing each node in order.
        Returns True if any node has updated its output.
        """
        self._has_update_flag = False
        for node in self.nodes:
            self._pump_node(node, dispatch_broker=dispatch_broker, reset_input=reset_input)
        return self._has_update_flag
    def clear_output_cache(self):
        self.output_cache.clear()
    def resume(self):
        for node in self.nodes:
            node.resume()
    def __repr__(self):
        from .op_graph_segment import _repr_graph
        node_info = {} # lets print cache state of each port
        for node in self.nodes:
            info_str="cache size: "
            cache_size = [len(self.output_cache.get((node, port), {})) for port in range(node.n_inputs)]
            info_str += str(cache_size)
            node_info[node] = info_str
        return _repr_graph("OpGraph()",self.nodes, self.edges, node_info)

__all__ = [
    "OpEdge",
    "OpGraph",
]

