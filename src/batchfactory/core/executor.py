from .base_op import BaseOp, PumpOutput, PumpOptions
from .entry import Entry
from .base_op import BaseOp
from .op_graph import OpGraphEdge, Graph

from typing import List, Tuple, NamedTuple, Dict, Set
from copy import deepcopy
import time


class OpGraphExecutor:
    def __init__(self, graph:Graph):
        self.reset_graph(graph)
        self.output_cache:Dict[Tuple[BaseOp,int],Dict[str,Entry]] = {}
        self.output_revs:Dict[Tuple[BaseOp,int],Dict[str,int]] = {}  # used to reject entry with the same revision emitted twice in the same run
        self.verbose=0
    def reset_graph(self, graph:Graph):
        self.graph = graph
    @property
    def nodes(self)->List[BaseOp]: return self.graph.nodes
    @property
    def edges(self)->List[OpGraphEdge]: return self.graph.edges
    @property
    def tail(self)->BaseOp: return self.graph.tail
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
            self.verbose>=2 and print(f"[OpGraphExecutor] Pumping node {node} with barrier level {node.barrier_level}")
            time_start = time.perf_counter()
            try:
                if options.max_barrier_level is not None and node.barrier_level > options.max_barrier_level:
                    continue
                did_emit = self._pump_node(node, options)
                if did_emit:
                    max_emitted_barrier_level = max(max_emitted_barrier_level or float('-inf'), node.barrier_level)
            except Exception as e:
                print(f"Exception while pumping node {node}: {e}")
                raise e
            time_end = time.perf_counter()
            self._time_prof[repr(node)] = self._time_prof.get(repr(node), 0) + (time_end - time_start)
        return max_emitted_barrier_level
    def clear_output_cache(self):
        self.output_revs.clear()
        self.output_cache.clear()
    def reset(self):
        self.clear_output_cache()
        for node in self.nodes:
            node.reset()
    def get_barrier_levels(self):
        return sorted(set(n.barrier_level for n in self.nodes))

    def execute(self, dispatch_brokers=False, mock=False, max_iterations = 1000, max_barrier_level:int|None = None, verbose=0, compact_after_finished=True):
        barrier_levels = sorted(barrier_level
            for barrier_level in {n.barrier_level for n in self.nodes} | {1}
            if max_barrier_level is None or barrier_level <= max_barrier_level
        )
        self.verbose = verbose
        self.verbose>=2 and print(f"[OpGraphExecutor] executing with barrier levels: {barrier_levels}")
        self.reset()
        first = True
        iterations = 0
        current_barrier_level_idx = 0
        self._time_prof = {}
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
        
        time_start = time.perf_counter()
        if compact_after_finished:
            for node in self.nodes:
                node.compact()
        time_end = time.perf_counter()
        self._time_prof["compact"] = time_end - time_start

        if self.verbose >= 1:
            self.show_node_times()

        # returns the output of output node
        output_objects = []
        for node in self.nodes:
            output = node.get_output()
            if output is not None:
                output_objects.append(output)
        if len(output_objects) == 1:
            return output_objects[0]
        elif len(output_objects) >1:
            return tuple(output_objects)
        else:
            return None
        
    def show_node_times(self):
        print("[OpGraphExecutor] Node times:")
        for name, time in sorted(self._time_prof.items(), key=lambda x: x[1], reverse=True):
            print(f"    {name}: {time:.4f} seconds")

    def get_node_output(self, node:BaseOp, port:int=None)->Dict[int,Dict[str,Entry]]|Dict[str,Entry]:
        if port is None:
            return {port: self.get_node_output(node, port) for port in range(node.n_out_ports)}
        return self.output_cache.get((node, port), {})


    def get_cache_summary(self)->Dict["BaseOp",str]:
        info_str = {}
        for node in self.nodes:
            cache_size = [len(self.output_cache.get((node, port), {})) for port in range(node.n_in_ports)]
            info_str[node] = f"cache size: {cache_size}"
        return info_str

__all__ = [
    "OpGraphExecutor",
]

