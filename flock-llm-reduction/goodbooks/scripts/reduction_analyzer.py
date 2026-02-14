"""
Query Tuple Reduction Analyzer
Based on: "Extending SQL to Return a Subdatabase" (Nix & Dittrich, 2025)

Implements the Yannakakis semi-join reduction algorithm for analyzing
tuple reduction in SQL queries with LLM functions.
"""

import re
import duckdb
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque
import argparse


class JoinGraph:
    """Represents the join graph of a query."""
    
    def __init__(self):
        self.nodes = set()  # table names
        self.edges = []  # list of (table1, table2, join_condition)
        self.aliases = {}  # table -> alias mapping

    def add_node(self, table: str, alias: Optional[str] = None):
        """Add a table node to the graph, with optional alias."""
        self.nodes.add(table)
        if alias and alias != table:
            self.aliases[table] = alias

    def add_edge(self, table1: str, table2: str, condition: str):
        """Add an edge (join) between two tables."""
        self.edges.append((table1, table2, condition))
        
    def is_cyclic(self) -> bool:
        """
        Check if join graph is cyclic.
        A graph is cyclic if: #edges >= #nodes
        """
        return len(self.edges) >= len(self.nodes)
    
    def get_neighbors(self, table: str) -> Set[str]:
        """Get all tables joined with this table."""
        neighbors = set()
        for t1, t2, _ in self.edges:
            if t1 == table:
                neighbors.add(t2)
            elif t2 == table:
                neighbors.add(t1)
        return neighbors
    
    def get_join_condition(self, table1: str, table2: str) -> Optional[str]:
        """Get join condition between two tables."""
        for t1, t2, cond in self.edges:
            if (t1 == table1 and t2 == table2) or (t1 == table2 and t2 == table1):
                return cond
        return None


class QueryReducer:
    """Analyzes tuple reduction using semi-join reduction algorithm."""
    
    def __init__(self, db_path: str = ":memory:"): 
        self.conn = duckdb.connect(db_path)
        self.table_sizes = {}
        
    def load_data_dynamic(self, data_dir: str):
        """Dynamically load ALL CSV files from directory."""
        data_path = Path(data_dir)
        
        if not data_path.exists():
            raise ValueError(f"Directory not found: {data_dir}")
        
        csv_files = list(data_path.glob("*.csv"))
        
        if not csv_files:
            raise ValueError(f"No CSV files found in: {data_dir}")
        
        print("=" * 70)
        print("Loading Data (Dynamic)")
        print("=" * 70)
        
        for csv_path in csv_files:
            table_name = csv_path.stem  # filename without .csv
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.execute(
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM read_csv_auto('{csv_path}')"
                )
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                self.table_sizes[table_name] = count
                print(f"✅ {table_name:<20} {count:>10,} rows")
            except Exception as e:
                print(f"❌ {table_name:<20} Error: {e}")
        
        print()
    
    def remove_llm_calls(self, query: str) -> str:
        """
        Remove all Flock LLM function calls from query.
        Handles: llm_filter, llm_complete, llm_reduce, llm_rerank, 
                 llm_first, llm_last, llm_embedding
        """
        # Case 1: conditions before the filter (e.g., "WHERE x = 1 AND llm_filter(...)")
        query = re.sub(
            r'\s+AND\s+llm_filter\s*\([^)]*\{[^}]*\}[^)]*\{[^}]*\}[^)]*\)',
            '', query, flags=re.DOTALL | re.IGNORECASE
        )
        
        # Case 2: conditions after the filter (e.g., "WHERE llm_filter(...) AND x = 1")
        query = re.sub(
            r'WHERE\s+llm_filter\s*\([^)]*\{[^}]*\}[^)]*\{[^}]*\}[^)]*\)\s+AND',
            'WHERE', query, flags=re.DOTALL | re.IGNORECASE
        )

        # Case 3: filter is the only condition (e.g., "WHERE llm_filter(...)")
        query = re.sub(
            r'WHERE\s+llm_filter\s*\([^)]*\{[^}]*\}[^)]*\{[^}]*\}[^)]*\)',
            '', query, flags=re.DOTALL | re.IGNORECASE
        )
        
        # Remove llm_* functions from SELECT clause
        llm_funcs = ['llm_complete', 'llm_reduce', 'llm_rerank', 'llm_first', 'llm_last', 'llm_embedding']
        for func in llm_funcs:
            pattern = rf',?\s*{func}\s*\([^)]*\{{[^}}]*\}}[^)]*\{{[^}}]*\}}[^)]*\)\s+AS\s+\w+'
            query = re.sub(pattern, '', query, flags=re.DOTALL | re.IGNORECASE)
        
        # Clean up trailing commas and whitespace
        query = re.sub(r',\s*FROM', ' FROM', query, flags=re.IGNORECASE)
        query = re.sub(r',\s*GROUP BY', ' GROUP BY', query, flags=re.IGNORECASE)
        query = re.sub(r',\s*ORDER BY', ' ORDER BY', query, flags=re.IGNORECASE)
        query = re.sub(r'SELECT\s+FROM', 'SELECT * FROM', query, flags=re.IGNORECASE)
        
        return query
    
    def parse_join_graph(self, query: str) -> JoinGraph:
        """Extract join graph from query (tables, joins, and conditions)."""
        graph = JoinGraph()
        
        # Extract FROM clause
        from_match = re.search(r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
        if from_match:
            table = from_match.group(1)
            alias = from_match.group(2) if from_match.group(2) else table
            graph.add_node(table, alias)
        
        # Extract JOINs
        join_pattern = r'(?:INNER\s+)?JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([^JOIN]+?)(?=(?:JOIN|WHERE|GROUP|ORDER|HAVING|LIMIT|$))'
        for match in re.finditer(join_pattern, query, re.IGNORECASE | re.DOTALL):
            table = match.group(1)
            alias = match.group(2) if match.group(2) else table
            join_cond = match.group(3).strip()
            
            graph.add_node(table, alias)
            
            # Parse join condition to find connected tables
            # e.g., "a.id = b.id" -> connects tables with aliases a and b
            cond_parts = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', join_cond)
            for alias1, col1, alias2, col2 in cond_parts:
                t1 = self._alias_to_table(alias1, graph)
                t2 = self._alias_to_table(alias2, graph)
                if t1 and t2:
                    graph.add_edge(t1, t2, join_cond)
        
        return graph
    
    def _alias_to_table(self, alias: str, graph: JoinGraph) -> Optional[str]:
        """Map alias back to actual table name."""
        for table, table_alias in graph.aliases.items():
            if table_alias == alias:
                return table
        # Check if alias is actually a table name
        if alias in graph.nodes:
            return alias
        return None
    
    def semi_join(self, left_table: str, right_table: str, join_condition: str):
        """
        Perform semi-join: left_table ⋉ right_table
        
        Semi-join keeps only rows from left_table that have matching rows 
        in right_table, defined as:
            SELECT DISTINCT left.* 
            FROM left 
            WHERE EXISTS (SELECT 1 FROM right WHERE join_condition)
        
        This REDUCES left_table to only tuples that join with right_table.
        """
        try:
            temp_name = f"{left_table}_reduced"
            
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_name}")
            self.conn.execute(f"""
                CREATE TABLE {temp_name} AS
                SELECT DISTINCT l.*
                FROM {left_table} l
                WHERE EXISTS (
                    SELECT 1 FROM {right_table} r
                    WHERE {join_condition}
                )
            """)
            
            # Replace original table with reduced version
            self.conn.execute(f"DROP TABLE {left_table}")
            self.conn.execute(f"ALTER TABLE {temp_name} RENAME TO {left_table}")
            
        except Exception as e:
            print(f"⚠ Semi-join error ({left_table} ⋉ {right_table}): {e}")
    
    def fold_cyclic_graph(self, graph: JoinGraph) -> JoinGraph:
        """
        Folding: Transform cyclic graph to acyclic
        
        If join graph has cycles (#edges >= #nodes):
            1. Repeatedly join pairs of connected tables
            2. Replace joined tables with their join result in the graph
            3. Continue until graph becomes acyclic (#edges < #nodes)
            4. Then apply Yannakakis algorithm
        
        Heuristic: Choose tables with highest degree (most connections)
        """
        while graph.is_cyclic():
            # Choose two connected tables with highest degrees
            degrees = {t: len(graph.get_neighbors(t)) for t in graph.nodes}
            table1 = max(degrees, key=degrees.get)
            neighbors = graph.get_neighbors(table1)
            
            if not neighbors:
                break
                
            table2 = max(neighbors, key=lambda t: degrees[t])
            
            # Join the two tables
            join_cond = graph.get_join_condition(table1, table2)
            if not join_cond:
                break
            
            joined_name = f"{table1}_JOIN_{table2}"
            
            try:
                self.conn.execute(f"""
                    CREATE TABLE {joined_name} AS
                    SELECT * FROM {table1} t1
                    JOIN {table2} t2 ON {join_cond}
                """)
                
                # Update graph: remove old tables, add joined table
                graph.nodes.remove(table1)
                graph.nodes.remove(table2)
                graph.nodes.add(joined_name)
                
                # Update edges to point to joined table
                new_edges = []
                for t1, t2, cond in graph.edges:
                    if t1 == table1 or t1 == table2:
                        new_edges.append((joined_name, t2, cond))
                    elif t2 == table1 or t2 == table2:
                        new_edges.append((t1, joined_name, cond))
                    else:
                        new_edges.append((t1, t2, cond))
                graph.edges = new_edges
                
            except Exception as e:
                print(f"⚠ Fold error: {e}")
                break
        
        return graph
    
    def yannakakis_reduction(self, graph: JoinGraph) -> Dict[str, Tuple[int, int, float]]:
        """
        Yannakakis' Semi-Join Reduction
        
        Given an acyclic join graph, reduce all tables to only tuples
        that participate in the final join result using semi-joins.
        
        Returns: Dict of {table_name: (original_size, reduced_size, reduction_pct)}
        """
        if not graph.nodes:
            return {}
        
        # ================================================================
        # STEP 0: Choose Root Node
        # ================================================================
        # Select an arbitrary node as root (heuristic: highest degree)
        # This establishes a tree structure for traversal
        
        degrees = {t: len(graph.get_neighbors(t)) for t in graph.nodes}
        root = max(degrees, key=degrees.get)
        
        # ================================================================
        # STEP 1: Bottom-Up Pass (Leaves → Root)
        # ================================================================
        # Perform BFS to establish traversal order, then traverse in REVERSE
        # For each (child, parent) pair: child ⋉ parent
        # This reduces each child to tuples that join with its parent
        
        visited = set()
        queue = deque([root])
        bfs_order = []
        
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            bfs_order.append(node)
            
            for neighbor in graph.get_neighbors(node):
                if neighbor not in visited:
                    queue.append(neighbor)
        
        # Traverse in REVERSE (bottom-up: leaves to root)
        for i in range(len(bfs_order) - 1, 0, -1):
            child = bfs_order[i]
            parent = bfs_order[i-1]
            join_cond = graph.get_join_condition(child, parent)
            if join_cond:
                # Rewrite join condition to use current table names
                join_cond_rewritten = join_cond.replace('l.', f'{child}.').replace('r.', f'{parent}.')
                self.semi_join(child, parent, join_cond_rewritten)
        
        # ================================================================
        # STEP 2: Top-Down Pass (Root → Leaves)
        # ================================================================
        # Traverse in FORWARD order (top-down: root to leaves)
        # For each (parent, child) pair: parent ⋉ child
        # This reduces each parent to tuples that join with its children
        
        for i in range(len(bfs_order) - 1):
            parent = bfs_order[i]
            child = bfs_order[i+1]
            join_cond = graph.get_join_condition(parent, child)
            if join_cond:
                join_cond_rewritten = join_cond.replace('l.', f'{parent}.').replace('r.', f'{child}.')
                self.semi_join(parent, child, join_cond_rewritten)
        
        # ================================================================
        # STEP 3: Calculate Reduction Statistics (Definition 2.2)
        # ================================================================
        # For each table Ti:
        #   - Original size: |Ti|
        #   - Reduced size: |Ti'| (after semi-join reduction)
        #   - Reduction %: ((|Ti| - |Ti'|) / |Ti|) × 100%
        
        reductions = {}
        for table in graph.nodes:
            original_size = self.table_sizes.get(table, 0)
            try:
                reduced_size = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except:
                reduced_size = 0
            
            if original_size > 0:
                reduction_pct = ((original_size - reduced_size) / original_size) * 100
            else:
                reduction_pct = 0.0
            
            reductions[table] = (original_size, reduced_size, reduction_pct)
        
        return reductions
    
    def compute_having_aware_reduction(self, query: str) -> Optional[Dict[str, Tuple[int, int, float]]]:
        """
        Handle queries with GROUP BY/HAVING by computing actual tuple participation.
        
        For patterns like:
            SELECT ... FROM (
                SELECT ... FROM child JOIN parent ON child.fk = parent.pk
                GROUP BY parent.pk, ...
                HAVING count(*) >= N
            ) AS x
        
        The reduction is computed by:
        1. Finding which parent PKs survive the HAVING filter
        2. Counting child rows that reference surviving parents
        """
        # Check for HAVING count(*) >= N pattern
        having_match = re.search(
            r'HAVING\s+count\s*\(\s*\*\s*\)\s*>=\s*(\d+)',
            query, re.IGNORECASE
        )
        
        if not having_match:
            return None  # No HAVING clause, use standard method
        
        min_count = int(having_match.group(1))
        
        # Find GROUP BY clause
        group_match = re.search(
            r'GROUP\s+BY\s+([^HAVING]+)',
            query, re.IGNORECASE | re.DOTALL
        )
        
        if not group_match:
            return None
        
        group_cols = group_match.group(1).strip()
        
        # Find the FROM ... JOIN ... ON pattern
        join_match = re.search(
            r'FROM\s+(\w+)\s+(\w+)\s+JOIN\s+(\w+)\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
            query, re.IGNORECASE
        )
        
        if not join_match:
            return None
        
        # Parse join info
        from_table = join_match.group(1)
        from_alias = join_match.group(2)
        join_table = join_match.group(3)
        join_alias = join_match.group(4)
        cond_alias1 = join_match.group(5)
        cond_col1 = join_match.group(6)
        cond_alias2 = join_match.group(7)
        cond_col2 = join_match.group(8)
        
        # Determine parent (grouped) vs child table
        # The table whose alias appears in GROUP BY is the parent
        if f"{from_alias}." in group_cols or group_cols.startswith(from_alias):
            parent_table, parent_alias = from_table, from_alias
            child_table, child_alias = join_table, join_alias
        elif f"{join_alias}." in group_cols or group_cols.startswith(join_alias):
            parent_table, parent_alias = join_table, join_alias
            child_table, child_alias = from_table, from_alias
        else:
            return None
        
        # Find the key columns from join condition
        if cond_alias1 == parent_alias:
            parent_pk_col = cond_col1
            child_fk_col = cond_col2
        else:
            parent_pk_col = cond_col2
            child_fk_col = cond_col1
        
        reductions = {}
        
        # Parent table: count how many pass the HAVING filter
        parent_original = self.table_sizes.get(parent_table, 0)
        try:
            parent_reduced = self.conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {parent_alias}.{parent_pk_col}
                    FROM {child_table} {child_alias}
                    JOIN {parent_table} {parent_alias} 
                      ON {child_alias}.{child_fk_col} = {parent_alias}.{parent_pk_col}
                    GROUP BY {parent_alias}.{parent_pk_col}
                    HAVING COUNT(*) >= {min_count}
                ) t
            """).fetchone()[0]
        except Exception as e:
            print(f"⚠ Error computing parent reduction: {e}")
            parent_reduced = parent_original
        
        if parent_original > 0:
            parent_pct = ((parent_original - parent_reduced) / parent_original) * 100
        else:
            parent_pct = 0.0
        reductions[parent_table] = (parent_original, parent_reduced, parent_pct)
        
        # Child table: count rows that reference surviving parents
        child_original = self.table_sizes.get(child_table, 0)
        try:
            child_reduced = self.conn.execute(f"""
                SELECT COUNT(*) FROM {child_table}
                WHERE {child_fk_col} IN (
                    SELECT {parent_alias}.{parent_pk_col}
                    FROM {child_table} {child_alias}
                    JOIN {parent_table} {parent_alias} 
                      ON {child_alias}.{child_fk_col} = {parent_alias}.{parent_pk_col}
                    GROUP BY {parent_alias}.{parent_pk_col}
                    HAVING COUNT(*) >= {min_count}
                )
            """).fetchone()[0]
        except Exception as e:
            print(f"⚠ Error computing child reduction: {e}")
            child_reduced = child_original
        
        if child_original > 0:
            child_pct = ((child_original - child_reduced) / child_original) * 100
        else:
            child_pct = 0.0
        reductions[child_table] = (child_original, child_reduced, child_pct)
        
        return reductions
    
    def analyze_query(self, query_file: str, show_queries: bool = True):
        """
        Pipeline:
        1. Remove LLM function calls from query
        2. Parse join graph from query
        3. If cyclic, fold until acyclic (Algorithm 3)
        4. Apply Yannakakis reduction (Algorithm 2)
        5. Report reduction statistics
        """
        query_path = Path(query_file)
        
        print("=" * 70)
        print(f"Query: {query_path.name}")
        print("=" * 70)
        print()
        
        with open(query_file, 'r') as f:
            original_query = f.read()
        
        if show_queries:
            print("ORIGINAL QUERY (with LLM functions):")
            print("-" * 70)
            print(original_query.strip())
            print()
        
        # Step 1: Remove LLM calls to get baseline SQL
        baseline_query = self.remove_llm_calls(original_query)
        
        if show_queries:
            print("BASELINE QUERY (LLM functions removed):")
            print("-" * 70)
            print(baseline_query.strip())
            print()
        
        # Step 2: Parse join graph from baseline query
        graph = self.parse_join_graph(baseline_query)
        
        if not graph.nodes:
            print("No tables found in query")
            return
        
        # Step 3: Handle cyclic graphs by folding
        if graph.is_cyclic():
            print(f"Join graph is CYCLIC ({len(graph.edges)} edges, {len(graph.nodes)} nodes)")
            print("   Applying folding algorithm...")
            graph = self.fold_cyclic_graph(graph)
            print(f"   ✅ Transformed to acyclic graph")
            print()
        
        # Step 4: Try HAVING-aware reduction first, then fall back to Yannakakis
        reductions = self.compute_having_aware_reduction(baseline_query)
        
        if reductions:
            print("Detected GROUP BY/HAVING pattern - using execution-based analysis")
            print()
        else:
            # Standard Yannakakis semi-join reduction
            reductions = self.yannakakis_reduction(graph)
        
        # Step 5: Report results
        print("TUPLE REDUCTION ANALYSIS:")
        print("-" * 70)
        print(f"{'Table':<20} {'Original':<12} {'Reduced':<12} {'Reduction %':<12}")
        print("-" * 70)
        
        total_original = 0
        total_reduced = 0
        
        for table in sorted(reductions.keys()):
            original, reduced, pct = reductions[table]
            print(f"{table:<20} {original:<12,} {reduced:<12,} {pct:>10.2f}%")
            total_original += original
            total_reduced += reduced
        
        print("-" * 70)
        
        if total_original > 0:
            overall_pct = ((total_original - total_reduced) / total_original) * 100
            print(f"{'OVERALL':<20} {total_original:<12,} {total_reduced:<12,} {overall_pct:>10.2f}%")
        
        print()


def main():
    parser = argparse.ArgumentParser(
        description='Analyze tuple reduction using Yannakakis semi-join algorithm',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a single query
  python reduction_analyzer.py query.sql --data-dir ./data/
  
  # Analyze multiple queries
  python reduction_analyzer.py queries/*.sql --data-dir ./data/
        """
    )
    
    parser.add_argument('query_files', nargs='+', help='SQL query file(s) to analyze')
    parser.add_argument('--data-dir', required=True, help='Directory containing CSV data files')
    
    args = parser.parse_args()
    
    reducer = QueryReducer()
    reducer.load_data_dynamic(args.data_dir)
    
    for query_file in args.query_files:
        try:
            reducer.analyze_query(query_file)
        except FileNotFoundError:
            print(f"❌ File not found: {query_file}\n")
        except Exception as e:
            print(f"❌ Error analyzing {query_file}: {e}\n")


if __name__ == '__main__':
    main()