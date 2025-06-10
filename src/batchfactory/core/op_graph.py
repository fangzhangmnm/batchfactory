from .op_base import BaseOp, PumpOutput, PumpOptions
from ..op.broker_op import BrokerOp
from .entry import Entry

from ..lib.utils import _pivot_cascaded_dict
from typing import List, Tuple, NamedTuple, Dict, Set
from copy import deepcopy

class OpGraphEdge(NamedTuple):
    source: BaseOp
    target: BaseOp
    source_port: int=0
    target_port: int=0

class OpGraph:
    def __init__(self, nodes:List[BaseOp], edges:List[OpGraphEdge]):
        # execution order is determined by order in the nodes array
        self.nodes = nodes
        self.edges = edges
        self.output_cache:Dict[Tuple[BaseOp,int],Dict[str,Entry]]={}
        self.output_revs:Dict[Tuple[BaseOp,int],Dict[str,int]]={}  # used to reject entry with the same revision emitted twice in the same run

    def _pump_node(self,node:BaseOp,options:PumpOptions)->bool:
        inputs:Dict[int,Dict[str,Entry]] = self._collect_node_inputs(node, use_deepcopy=True)
        pump_output:PumpOutput = node.pump(inputs=inputs, options=options)
        self._update_node_outputs(node, pump_output.outputs)
        self._consume_node_inputs(node, pump_output.consumed)
        return pump_output.did_emit

    def incoming_edge(self,node,port)->OpGraphEdge:
        for edge in self.edges:
            if edge.target == node and edge.target_port == port:
                return edge
        return None
    def incoming_edges(self,node)->List[OpGraphEdge]:
        return [edge for edge in self.edges if edge.target == node]
    def outgoing_edges(self,node)->List[OpGraphEdge]:
        return [edge for edge in self.edges if edge.source == node]

    def _collect_node_inputs(self,node:BaseOp,use_deepcopy:bool)->Dict[int,Dict[str,Entry]]:
        inputs:Dict[int,Dict[str,Entry]] = {port:{} for port in range(node.n_in_ports)}
        for edge in self.incoming_edges(node):
            port_inputs = self.output_cache.setdefault((edge.source, edge.source_port), {})
            for idx, entry in port_inputs.items():
                if use_deepcopy:
                    entry = deepcopy(entry)
                inputs.setdefault(edge.target_port, {})[idx] = entry
        return inputs
    
    def _consume_node_inputs(self,node,consumed:Dict[int,Set[str]]):
        for port, idxs in consumed.items():
            for idx in idxs:
                self._consume_node_input(node, port, idx)
    
    def _consume_node_input(self,node,port,idx):
        edge = self.incoming_edge(node, port)
        if edge is None: return
        src_entries = self.output_cache.setdefault((edge.source, edge.source_port), {})
        if idx in src_entries:
            del src_entries[idx]

    def _update_node_outputs(self,node,outputs:Dict[int,Dict[str,Entry]]):
        for port,batch in outputs.items():
            for idx, entry in batch.items():
                self._update_node_output(node, port, idx, entry)
    
    def _update_node_output(self,node,port,idx,entry):
        port_entries = self.output_cache.setdefault((node, port), {})
        port_revs = self.output_revs.setdefault((node, port), {})
        if idx in port_revs and entry.rev <= port_revs[idx]:
            return
        if idx not in port_entries or entry.rev >= port_entries[idx].rev:
            port_entries[idx] = entry
            port_revs[idx] = entry.rev
            self._has_update_flag = True

    def pump(self, options:PumpOptions)->bool:
        """ 
        Pump the graph, processing each node in order.
        Returns True if any node has updated its output.
        """
        did_emit = False
        for node in self.nodes:
            if self._pump_node(node,options):
                did_emit = True
        return did_emit
    def clear_output_cache(self):
        self.output_cache.clear()
    def resume(self):
        for node in self.nodes:
            node.resume()
    
    def execute(self, dispatch_brokers=False, mock=False, max_iterations = 1000):
        "clear output cache, resume, load inputs, and pump until no more updates"
        self.clear_output_cache()
        self.resume()
        first = True
        did_emit = True
        iterations = 0
        while True:
            while True:
                if iterations >= max_iterations: break
                did_emit = self.pump(PumpOptions(
                    dispatch_brokers = False,
                    mock = mock,
                    reload_inputs = first))
                iterations +=1
                first = False
                if not did_emit: break
            if iterations >= max_iterations: break
            did_emit = self.pump(PumpOptions(
                dispatch_brokers=dispatch_brokers,
                mock=mock,
                reload_inputs=False))
            iterations += 1
            if not did_emit: break

    def __repr__(self):
        from .op_graph_segment import _repr_graph
        node_info = {} # lets print cache state of each port
        for node in self.nodes:
            info_str="cache size: "
            cache_size = [len(self.output_cache.get((node, port), {})) for port in range(node.n_in_ports)]
            info_str += str(cache_size)
            node_info[node] = info_str
        return _repr_graph("OpGraph()",self.nodes, self.edges, node_info)

__all__ = [
    "OpGraphEdge",
    "OpGraph",
]

