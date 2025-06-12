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
    def __init__(self, nodes:List[BaseOp], edges:List[OpGraphEdge],tail:BaseOp=None):
        # execution order is determined by order in the nodes array
        self.nodes = nodes
        self.edges = edges
        self.output_cache:Dict[Tuple[BaseOp,int],Dict[str,Entry]]={}
        self.output_revs:Dict[Tuple[BaseOp,int],Dict[str,int]]={}  # used to reject entry with the same revision emitted twice in the same run
        self.tail = tail
    def _pump_node(self,node:BaseOp,options:PumpOptions)->bool:
        if options.max_barrier_level is not None and node.barrier_level > options.max_barrier_level:
            return False
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

    def pump(self, options:PumpOptions)->int:
        """ 
        Pump the graph, processing each node in order.
        Returns the max barrier level of the node that emitted an update, or None if no updates were emitted.
        """
        max_emitted_barrier_level = None
        for node in self.nodes:
            if options.max_barrier_level is not None and node.barrier_level > options.max_barrier_level:
                continue
            did_emit = self._pump_node(node, options)
            if did_emit:
                max_emitted_barrier_level = max(max_emitted_barrier_level or float('-inf'), node.barrier_level)
        return max_emitted_barrier_level
    def clear_output_cache(self):
        self.output_cache.clear()
    def resume(self):
        for node in self.nodes:
            node.resume()
    
    # def execute(self, dispatch_brokers=False, mock=False, max_iterations = 1000):
    #     "clear output cache, resume, load inputs, and pump until no more updates"
    #     self.clear_output_cache()
    #     self.resume()
    #     first = True
    #     did_emit = True
    #     iterations = 0
    #     while True:
    #         while True:
    #             if iterations >= max_iterations: break
    #             did_emit = self.pump(PumpOptions(
    #                 dispatch_brokers = False,
    #                 mock = mock,
    #                 reload_inputs = first))
    #             iterations +=1
    #             first = False
    #             if not did_emit: break
    #         if iterations >= max_iterations: break
    #         did_emit = self.pump(PumpOptions(
    #             dispatch_brokers=dispatch_brokers,
    #             mock=mock,
    #             reload_inputs=False))
    #         iterations += 1
    #         if not did_emit: break
    #     return list(self.output_cache.get((self.tail, 0), {}).values()) if self.tail else []

    def get_barrier_levels(self):
        return sorted(set(n.barrier_level for n in self.nodes))

    def execute(self, dispatch_brokers=False, mock=False, max_iterations = 1000, max_barrier_level:int|None = None):
        barrier_levels = sorted(barrier_level
            for barrier_level in {n.barrier_level for n in self.nodes} | {1}
            if max_barrier_level is None or barrier_level <= max_barrier_level
        )
        self.clear_output_cache()
        self.resume()
        first = True
        iterations = 0
        current_barrier_level_idx = 0
        while True:
            current_barrier_level = barrier_levels[current_barrier_level_idx]
            emit_level = self.pump(PumpOptions(
                dispatch_brokers=(current_barrier_level>0) and dispatch_brokers,
                mock=mock,
                reload_inputs=first,
                max_barrier_level=current_barrier_level))
            iterations += 1
            first = False
            if emit_level is None:
                if current_barrier_level_idx < len(barrier_levels) - 1:
                    current_barrier_level_idx += 1
                    continue
                else:
                    break
            else:
                current_barrier_level_idx = min(current_barrier_level_idx, barrier_levels.index(emit_level))
            if iterations >= max_iterations:
                break
        if self.tail:
            return list(self.output_cache.get((self.tail, 0), {}).values())



                
            


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

