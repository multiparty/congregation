from congregation.dag.nodes import OpNode


class Dag:
    def __init__(self, roots: set):
        self.roots = roots

    def __str__(self):
        return "\n".join(str(node) for node in self.top_sort())

    def involves_compute_party(self, pid: int):
        """
        For a given PID, check if it owns any
        data associated with this DAG
        """

        for r in self.roots:
            for sw_set in r.out_rel.stored_with:
                if pid in sw_set:
                    return True
        return False

    def dfs_visit(self, visitor):

        visited = set()
        for root in self.roots:
            self._dfs_visit(root, visitor, visited)

        return visited

    def _dfs_visit(self, node: OpNode, visitor, visited: set):

        visitor(node)
        visited.add(node)
        for child in node.children:
            if child not in visited:
                self._dfs_visit(child, visitor, visited)

    def dfs_print(self):
        self.dfs_visit(print)

    def get_all_nodes(self):
        return self.dfs_visit(lambda node: node)

    def top_sort(self):

        unmarked = sorted(list(self.get_all_nodes()), key=lambda x: x.out_rel.name)
        marked = set()
        temp_marked = set()
        ordered = []

        while unmarked:
            node = unmarked.pop()
            self._top_sort_visit(node, marked, temp_marked, unmarked, ordered)

        return ordered

    def _top_sort_visit(self, node: OpNode, marked: set, temp_marked: set, unmarked: [list, set], ordered: list):

        if node in temp_marked:
            raise Exception(f"Cycle detected in graph, not a dag: Node {node} was in {temp_marked}.")

        if node not in marked:
            if node in unmarked:
                unmarked.remove(node)
            temp_marked.add(node)
            children = sorted(list(node.children), key=lambda x: x.out_rel.name)

            for other_node in children:
                self._top_sort_visit(other_node, marked, temp_marked, unmarked, ordered)

            marked.add(node)
            unmarked.append(node)
            temp_marked.remove(node)
            ordered.insert(0, node)
