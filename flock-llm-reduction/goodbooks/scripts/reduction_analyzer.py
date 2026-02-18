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
        A graph is cyclic if: #edges >= #nodes (and graph is non-empty)
        """
        if not self.nodes:
            return False
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
            table_name = csv_path.stem # filename without .csv
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.execute(
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM read_csv_auto('{csv_path}')"
                )
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0] # fetchone() returns a tuple like (count,), so we take [0]
                self.table_sizes[table_name] = count
                print(f"✅ {table_name:<20} {count:>10,} rows") # :> and :< for alignment
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
        # [^)] means any number of characters that are not a closing parenthesis
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
    
    def _flatten_subqueries(self, query: str) -> str:
        """
        Replace the bodies of all nested parenthesised subqueries with the
        placeholder token ``_sq_`` so that regex-based parsing can safely
        operate on the top-level SQL structure only.

        Example:
            ``FROM (SELECT * FROM foo) t  JOIN bar ON ...``
            becomes
            ``FROM (_sq_) t  JOIN bar ON ...``
        """
        result = []
        depth = 0
        for c in query:
            if c == '(':
                if depth == 0:
                    result.append('(_sq_)') # placeholder for the whole subquery group
                depth += 1
            elif c == ')':
                depth -= 1
            else:
                if depth == 0:
                    result.append(c)
        return ''.join(result)

    def _extract_base_query(self, query: str) -> str:
        """
        If the outermost FROM clause is a subquery wrapper
        (``FROM (SELECT …) alias``) and no additional table JOINs follow it at
        the same level, drill recursively into the subquery until we reach a
        level that either (a) has a table name directly after FROM, or (b) has
        explicit ``JOIN <table>`` clauses after an inner subquery.

        This handles patterns like::

            SELECT … FROM (SELECT … FROM books b JOIN …) candidates

        so that ``parse_join_graph`` can see the base tables.
        """
        # Walk at depth 0 to find the top-level FROM keyword
        depth = 0
        from_pos = -1
        i = 0
        while i < len(query):
            c = query[i]
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            elif depth == 0 and query[i:i + 4].upper() == 'FROM':
                prev_ok = (i == 0 or not query[i - 1].isalpha())
                next_ok = (i + 4 >= len(query) or not query[i + 4].isalpha())
                if prev_ok and next_ok:
                    from_pos = i
                    break
            i += 1

        # If FROM keyword not found at depth 0, return query unchanged
        if from_pos == -1:
            return query

        # Skip whitespace after FROM
        j = from_pos + 4
        while j < len(query) and query[j] in ' \t\n\r':
            j += 1

        if j >= len(query) or query[j] != '(':
            return query # FROM is followed by a table name, nothing to unwrap

        # Extract the subquery body (matching parens)
        j += 1 # step past the opening '('
        depth = 1
        start = j
        while j < len(query) and depth > 0:
            if query[j] == '(':
                depth += 1
            elif query[j] == ')':
                depth -= 1
            j += 1

        inner = query[start:j - 1].strip()
        rest_of_query = query[j:]

        # If the text that follows the subquery at this level contains explicit
        # table JOINs (e.g. ``JOIN books a ON …``) we need to stay at this
        # level so those JOINs are visible to the parser.
        if re.search(r'\bJOIN\s+\w', rest_of_query, re.IGNORECASE): # \w matches table name after JOIN
            return query

        # No table JOINs at this level – recurse into the subquery
        return self._extract_base_query(inner)

    def parse_join_graph(self, query: str) -> JoinGraph:
        """Extract join graph from query (tables, joins, and conditions)."""
        graph = JoinGraph()

        # Normalize query structure for parsing
        query = self._extract_base_query(query)
        flat_query = self._flatten_subqueries(query)

        # SQL keywords that must never be mistaken for an alias
        SQL_KEYWORDS = {
            'JOIN', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT',
            'ON', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS',
            'SELECT', 'FROM', 'AS', 'SET', 'AND', 'OR', 'NOT',
        }

        # Extract FROM clause (operates on the flattened query so that the inner
        # FROM clauses inside subqueries are not accidentally matched
        from_match = re.search(r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', flat_query, re.IGNORECASE)
        if from_match:
            table = from_match.group(1)
            raw_alias = from_match.group(2)
            # Discard captured word if it is a SQL keyword
            alias = (raw_alias if raw_alias and raw_alias.upper() not in SQL_KEYWORDS
                     else table)
            graph.add_node(table, alias)

        # To find each JOIN block like: JOIN table_name [AS alias] ON <join condition>
        join_pattern = (
            r'(?:INNER\s+)?JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+'
            r'(.*?)'
            r'(?=\s+(?:INNER\s+)?JOIN\b|\s+WHERE\b|\s+GROUP\b'
            r'|\s+ORDER\b|\s+HAVING\b|\s+LIMIT\b|\s*$)'
        )

        for match in re.finditer(join_pattern, flat_query, re.IGNORECASE | re.DOTALL):
            table = match.group(1)
            raw_alias = match.group(2)
            # Discard captured word if it is a SQL keyword
            alias = (raw_alias if raw_alias and raw_alias.upper() not in SQL_KEYWORDS
                     else table)
            join_cond = match.group(3).strip() # E.g. "a.id = b.author_id"
            
            graph.add_node(table, alias)
            
            # Parse join condition to find connected tables
            # e.g., "a.id = b.id" -> connects tables with aliases a and b
            cond_parts = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', join_cond)
            for alias1, col1, alias2, col2 in cond_parts:
                t1 = self._alias_to_table(alias1, graph)
                t2 = self._alias_to_table(alias2, graph)
                if t1 and t2:
                    # Normalize condition to use table names instead of aliases
                    # E.g. "b.id = a.id" becomes "books.id = authors.id" if b->books, a->authors
                    cond_normalized = re.sub(
                        rf'\b{re.escape(alias1)}\.', f'{t1}.', join_cond
                    )
                    cond_normalized = re.sub(
                        rf'\b{re.escape(alias2)}\.', f'{t2}.', cond_normalized
                    )
                    graph.add_edge(t1, t2, cond_normalized)
        
        return graph
    
    def _alias_to_table(self, alias: str, graph: JoinGraph) -> Optional[str]:
        """
        Map alias back to actual table name.

        This is needed because join conditions may reference table aliases (e.g., 'a.id', 'b.id')
        but we need to normalize them to actual table names (e.g., 'books.id', 'authors.id')
        for semi-join operations to work correctly.
        """
        for table, table_alias in graph.aliases.items(): # .items() gives the (k, v) pairs
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
                # Rewrite join condition to use t1/t2 aliases
                fold_cond = re.sub(rf'\b{re.escape(table1)}\.', 't1.', join_cond)
                fold_cond = re.sub(rf'\b{re.escape(table2)}\.', 't2.', fold_cond)
                self.conn.execute(f"""
                    CREATE TABLE {joined_name} AS
                    SELECT * FROM {table1} t1
                    JOIN {table2} t2 ON {fold_cond}
                """)
                
                # Update graph: remove old tables, add joined table
                graph.nodes.remove(table1)
                graph.nodes.remove(table2)
                graph.nodes.add(joined_name)
                
                # Update edges: drop the edge between the two folded tables,
                # redirect remaining edges to the joined table, and rewrite
                # conditions so old table names point to the joined table.
                new_edges = []
                for t1, t2, cond in graph.edges:
                    # Skip the edge that connected the two now-merged tables
                    if {t1, t2} == {table1, table2}:
                        continue
                    # Rewrite old table names in condition to joined_name
                    cond = re.sub(rf'\b{re.escape(table1)}\.', f'{joined_name}.', cond)
                    cond = re.sub(rf'\b{re.escape(table2)}\.', f'{joined_name}.', cond)
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
        # BFS to build the traversal order AND record each node's parent in
        # the BFS spanning tree.  Using consecutive BFS pairs as parent/child
        # is only correct for chains; for general trees the parent pointer
        # must be tracked explicitly.
        
        visited = set()
        queue = deque([root])
        bfs_order = []
        # Track explicit parent relationships
        parent_of: Dict[str, Optional[str]] = {root: None}
        
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            bfs_order.append(node)
            
            for neighbor in graph.get_neighbors(node):
                if neighbor not in visited:
                    # Record parent before enqueueing
                    parent_of[neighbor] = node
                    queue.append(neighbor)
        
        def _rewrite_cond(cond: str, left_table: str, right_table: str) -> str:
            """
            Replace exact table-name prefixes with the l./r. aliases that
            semi_join() expects.

            Using re.sub with a word boundary (\b) prevents a shorter name
            that is a suffix of a longer one (e.g. 'tags' inside 'book_tags')
            from being incorrectly replaced by plain str.replace.
            """
            cond = re.sub(rf'\b{re.escape(left_table)}\.', 'l.', cond)
            cond = re.sub(rf'\b{re.escape(right_table)}\.', 'r.', cond)
            return cond

        # Traverse in REVERSE (bottom-up: leaves to root)
        # For each child, reduce its PARENT: parent ⋉ child
        for node in reversed(bfs_order[1:]): # skip root (index 0)
            parent = parent_of[node]
            join_cond = graph.get_join_condition(parent, node)
            if join_cond:
                self.semi_join(parent, node, _rewrite_cond(join_cond, parent, node))
        
        # ================================================================
        # STEP 2: Top-Down Pass (Root → Leaves)
        # ================================================================
        # Traverse in FORWARD order (top-down: root to leaves)
        # For each child, reduce the CHILD: child ⋉ parent
        
        for node in bfs_order[1:]: # skip root
            parent = parent_of[node]
            join_cond = graph.get_join_condition(node, parent)
            if join_cond:
                self.semi_join(node, parent, _rewrite_cond(join_cond, node, parent))
        
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
        # Also match HAVING COUNT(DISTINCT col) >= N
        having_match = re.search(
            r'HAVING\s+count\s*\(\s*(?:DISTINCT\s+[\w.]+|\*)\s*\)\s*>=\s*(\d+)',
            query, re.IGNORECASE
        )
        
        if not having_match:
            return None # No HAVING clause, use standard method
        
        min_count = int(having_match.group(1))
        
        # Find GROUP BY clause
        group_match = re.search(
            r'GROUP\s+BY\s+(.*?)(?=\s+HAVING\b)',
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
    
    def _apply_local_predicates(self, base_query: str, graph: 'JoinGraph'):
        """
        Apply non-join WHERE predicates to pre-filter tables in place BEFORE
        Yannakakis semi-joins run.

        This is the "selection pushdown" step that Yannakakis requires:
        local predicates (those referencing only one table) must be applied
        first so the algorithm can propagate the resulting reduction through
        the rest of the join graph via semi-joins.

        Example: WHERE lower(t.tag_name) LIKE '%mystery%'
            -> filters `tags` down to only mystery-related tags FIRST
            -> semi-joins then cascade that reduction to book_tags, then books

        Without this step, every table in a densely-connected schema shows
        0 % reduction because almost every row joins with something.
        """
        # Build alias -> table mapping from graph.aliases (which stores table->alias)
        alias_to_table = {alias: table for table, alias in graph.aliases.items()}
        # Tables with no alias use their own name, add them too
        for table in graph.nodes:
            if table not in graph.aliases:
                alias_to_table[table] = table

        # Extract the WHERE clause body from the base query
        where_match = re.search(
            r'\bWHERE\b(.*?)(?=\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|\s*$)',
            base_query, re.IGNORECASE | re.DOTALL
        )
        if not where_match:
            return
        where_body = where_match.group(1).strip()

        # Process tables that have an explicit alias
        for table, alias in graph.aliases.items():
            alias_pat = rf'\b{re.escape(alias)}\.' 
            if not re.search(alias_pat, where_body, re.IGNORECASE):
                continue
            # Skip if another join-table alias also appears (would be a join condition)
            other_aliases = [a for t, a in graph.aliases.items() if t != table and a != alias]
            if any(re.search(rf'\b{re.escape(oa)}\.', where_body, re.IGNORECASE)
                   for oa in other_aliases):
                continue
            # Rewrite alias.col -> table.col and apply as a filter
            predicate = re.sub(alias_pat, f'{table}.', where_body, flags=re.IGNORECASE)
            try:
                temp = f"{table}_filtered"
                self.conn.execute(f"DROP TABLE IF EXISTS {temp}")
                self.conn.execute(f"CREATE TABLE {temp} AS SELECT * FROM {table} WHERE {predicate}")
                self.conn.execute(f"DROP TABLE {table}")
                self.conn.execute(f"ALTER TABLE {temp} RENAME TO {table}")
            except Exception as e:
                print(f"  Could not apply local predicate to {table}: {e}")

        # Process tables with no alias (referenced directly as tablename.col)
        for table in graph.nodes:
            if table in graph.aliases:
                continue  # already handled above
            tbl_pat = rf'\b{re.escape(table)}\.' 
            if not re.search(tbl_pat, where_body, re.IGNORECASE):
                continue
            other_tables = [t for t in graph.nodes if t != table]
            if any(re.search(rf'\b{re.escape(ot)}\.', where_body, re.IGNORECASE)
                   for ot in other_tables):
                continue
            try:
                temp = f"{table}_filtered"
                self.conn.execute(f"DROP TABLE IF EXISTS {temp}")
                self.conn.execute(f"CREATE TABLE {temp} AS SELECT * FROM {table} WHERE {where_body}")
                self.conn.execute(f"DROP TABLE {table}")
                self.conn.execute(f"ALTER TABLE {temp} RENAME TO {table}")
            except Exception as e:
                print(f"  Could not apply local predicate to {table}: {e}")

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
        
        # LIMIT: the LLM only processes at most N rows regardless of table sizes
        limit_match = re.search(r'\bLIMIT\s+(\d+)\b', baseline_query, re.IGNORECASE)
        if limit_match:
            limit_n = int(limit_match.group(1))
            print(f"⚠ Note: Query contains LIMIT {limit_n:,}.")
            print(f"   The LLM function will process at most {limit_n:,} result rows,")
            print(f"   regardless of the table-level reduction percentages shown below.")
            print()

        # CROSS JOIN: Cartesian products can't be reduced by semi-joins
        if re.search(r'\bCROSS\s+JOIN\b', baseline_query, re.IGNORECASE):
            print("⚠ Note: Query contains a CROSS JOIN.")
            print("   Yannakakis semi-join reduction does not apply to Cartesian products.")
            print("   Tuple counts shown below are the *full* table sizes (0 % reduction).")
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
            # Apply local WHERE predicates first (selection pushdown)
            base_query_for_preds = self._extract_base_query(baseline_query)
            self._apply_local_predicates(base_query_for_preds, graph)
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