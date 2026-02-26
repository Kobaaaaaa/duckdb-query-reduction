"""
Comprehensive unit tests for reduction_analyzer.py
"""

import pytest
import duckdb
from reduction_analyzer import JoinGraph, QueryReducer

# ================================
# Fixtures
# ================================

@pytest.fixture
def graph():
    """Empty JoinGraph."""
    return JoinGraph()


@pytest.fixture
def reducer():
    """QueryReducer with in-memory DuckDB."""
    return QueryReducer(db_path=":memory:")


@pytest.fixture
def reducer_with_two_tables(reducer):
    """
    Two tables: orders(id, customer_id, amount) and customers(id, name).
    Some orders reference customers that exist, some don't.
    """
    reducer.conn.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    reducer.conn.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Diana')
    """)
    reducer.conn.execute("CREATE TABLE orders (id INT, customer_id INT, amount FLOAT)")
    reducer.conn.execute("""
        INSERT INTO orders VALUES
        (10, 1, 100.0), (11, 1, 200.0), (12, 2, 50.0),
        (13, 5, 75.0),  (14, 6, 300.0)
    """)
    reducer.table_sizes = {"customers": 4, "orders": 5}
    return reducer


@pytest.fixture
def reducer_chain(reducer):
    """
    Three-table chain: A --fk--> B --fk--> C
    A(id, b_id), B(id, c_id), C(id)
    Some rows don't participate in the full join.
    """
    reducer.conn.execute("CREATE TABLE C (id INT)")
    reducer.conn.execute("INSERT INTO C VALUES (1), (2)")

    reducer.conn.execute("CREATE TABLE B (id INT, c_id INT)")
    reducer.conn.execute("""
        INSERT INTO B VALUES (10, 1), (11, 2), (12, 3), (13, 999)
    """)

    reducer.conn.execute("CREATE TABLE A (id INT, b_id INT)")
    reducer.conn.execute("""
        INSERT INTO A VALUES (100, 10), (101, 11), (102, 12),
                             (103, 13), (104, 777), (105, 888)
    """)

    reducer.table_sizes = {"A": 6, "B": 4, "C": 2}
    return reducer


@pytest.fixture
def reducer_star(reducer):
    """
    Star schema: center table joined to 3 leaf tables.
    center(id), leaf1(id, center_id), leaf2(id, center_id), leaf3(id, center_id)
    """
    reducer.conn.execute("CREATE TABLE center (id INT)")
    reducer.conn.execute("INSERT INTO center VALUES (1), (2), (3), (4), (5)")

    reducer.conn.execute("CREATE TABLE leaf1 (id INT, center_id INT)")
    reducer.conn.execute("INSERT INTO leaf1 VALUES (10, 1), (11, 2), (12, 99)")

    reducer.conn.execute("CREATE TABLE leaf2 (id INT, center_id INT)")
    reducer.conn.execute("INSERT INTO leaf2 VALUES (20, 2), (21, 3), (22, 88)")

    reducer.conn.execute("CREATE TABLE leaf3 (id INT, center_id INT)")
    reducer.conn.execute("INSERT INTO leaf3 VALUES (30, 1), (31, 77)")

    reducer.table_sizes = {"center": 5, "leaf1": 3, "leaf2": 3, "leaf3": 2}
    return reducer


# ================================
# JoinGraph Tests
# ================================

class TestJoinGraph:

    def test_add_node_basic(self, graph):
        graph.add_node("books")
        assert "books" in graph.nodes
        assert graph.aliases == {}

    def test_add_node_with_alias(self, graph):
        graph.add_node("books", "b")
        assert "books" in graph.nodes
        assert graph.aliases == {"books": "b"}

    def test_add_node_alias_same_as_table_is_ignored(self, graph):
        graph.add_node("books", "books")
        assert "books" in graph.nodes
        assert graph.aliases == {}

    def test_add_node_none_alias(self, graph):
        graph.add_node("books", None)
        assert graph.aliases == {}

    def test_add_edge(self, graph):
        graph.add_node("a")
        graph.add_node("b")
        graph.add_edge("a", "b", "a.id = b.a_id")
        assert len(graph.edges) == 1
        assert graph.edges[0] == ("a", "b", "a.id = b.a_id")

    def test_is_cyclic_empty(self, graph):
        assert not graph.is_cyclic()

    def test_is_cyclic_tree(self, graph):
        """Tree: 3 nodes, 2 edges -> not cyclic."""
        graph.nodes = {"a", "b", "c"}
        graph.edges = [("a", "b", ""), ("b", "c", "")]
        assert not graph.is_cyclic()  # 2 < 3

    def test_is_cyclic_triangle(self, graph):
        """Triangle: 3 nodes, 3 edges -> cyclic."""
        graph.nodes = {"a", "b", "c"}
        graph.edges = [("a", "b", ""), ("b", "c", ""), ("a", "c", "")]
        assert graph.is_cyclic()  # 3 >= 3

    def test_is_cyclic_single_node_no_edge(self, graph):
        graph.nodes = {"a"}
        graph.edges = []
        assert not graph.is_cyclic()

    def test_is_cyclic_two_nodes_two_edges(self, graph):
        """Multi-edge between same pair: 2 nodes, 2 edges -> cyclic."""
        graph.nodes = {"a", "b"}
        graph.edges = [("a", "b", "x"), ("a", "b", "y")]
        assert graph.is_cyclic()  # 2 >= 2

    def test_get_neighbors(self, graph):
        graph.nodes = {"a", "b", "c", "d"}
        graph.edges = [("a", "b", ""), ("a", "c", ""), ("b", "d", "")]
        assert graph.get_neighbors("a") == {"b", "c"}
        assert graph.get_neighbors("b") == {"a", "d"}
        assert graph.get_neighbors("d") == {"b"}

    def test_get_neighbors_isolated(self, graph):
        graph.nodes = {"a", "b"}
        graph.edges = []
        assert graph.get_neighbors("a") == set()

    def test_get_join_condition_found(self, graph):
        graph.edges = [("x", "y", "x.id = y.x_id")]
        assert graph.get_join_condition("x", "y") == "x.id = y.x_id"
        # Reverse order should also work
        assert graph.get_join_condition("y", "x") == "x.id = y.x_id"

    def test_get_join_condition_not_found(self, graph):
        graph.edges = [("x", "y", "cond")]
        assert graph.get_join_condition("x", "z") is None


# ================================
# LLM Call Removal Tests
# ================================

class TestRemoveLlmCalls:

    def test_llm_filter_only_condition(self, reducer):
        query = "SELECT * FROM t WHERE llm_filter('{prompt}', '{model}')"
        result = reducer.remove_llm_calls(query)
        assert "llm_filter" not in result
        assert "WHERE" not in result
        assert result.strip() == "SELECT * FROM t"

    def test_llm_filter_with_and_before(self, reducer):
        query = "SELECT * FROM t WHERE x = 1 AND llm_filter('{p}', '{m}')"
        result = reducer.remove_llm_calls(query)
        assert "llm_filter" not in result
        assert "x = 1" in result
        assert result.strip() == "SELECT * FROM t WHERE x = 1"


    def test_llm_filter_with_and_after(self, reducer):
        query = "SELECT * FROM t WHERE llm_filter('{p}', '{m}') AND x = 1"
        result = reducer.remove_llm_calls(query)
        assert "llm_filter" not in result
        assert "x = 1" in result
        assert result.strip() == "SELECT * FROM t WHERE x = 1"

    def test_llm_complete_in_select(self, reducer):
        query = "SELECT name, llm_complete('{p}', '{m}') AS summary FROM t"
        result = reducer.remove_llm_calls(query)
        assert "llm_complete" not in result
        assert "name" in result
        assert result.strip() == "SELECT name FROM t"

    def test_no_llm_calls_unchanged(self, reducer):
        query = "SELECT * FROM books b JOIN authors a ON b.author_id = a.id"
        result = reducer.remove_llm_calls(query)
        assert result.strip() == query.strip()

    def test_select_only_llm_becomes_star(self, reducer):
        """If removing LLM functions empties the SELECT, it becomes SELECT *."""
        query = "SELECT llm_complete('{p}', '{m}') AS out FROM t"
        result = reducer.remove_llm_calls(query)
        assert "SELECT *" in result or "SELECT  FROM" not in result
        assert result.strip() == "SELECT * FROM t"

    def test_trailing_comma_cleanup(self, reducer):
        query = "SELECT a, llm_complete('{p}', '{m}') AS out FROM t"
        result = reducer.remove_llm_calls(query)
        # Should not have ", FROM"
        assert ", FROM" not in result.upper()
        assert ",  FROM" not in result.upper()
        assert result.strip() == "SELECT a FROM t"


# ================================
# Flatten Subqueries Tests
# ================================

class TestFlattenSubqueries:

    def test_no_parens(self, reducer):
        q = "SELECT * FROM books b JOIN authors a ON b.aid = a.id"
        assert reducer._flatten_subqueries(q) == q

    def test_single_subquery(self, reducer):
        q = "SELECT * FROM (SELECT * FROM foo) t JOIN bar ON t.id = bar.id"
        flat = reducer._flatten_subqueries(q)
        assert "(_sq_)" in flat
        assert "SELECT * FROM foo" not in flat
        assert "bar" in flat
        assert flat == "SELECT * FROM (_sq_) t JOIN bar ON t.id = bar.id"

    def test_nested_subqueries(self, reducer):
        q = "SELECT * FROM (SELECT * FROM (SELECT 1) inner_t) outer_t"
        flat = reducer._flatten_subqueries(q)
        # Only one _sq_ at top level (inner parens are consumed)
        assert flat.count("(_sq_)") == 1
        assert "SELECT 1" not in flat
        assert flat == "SELECT * FROM (_sq_) outer_t"

    def test_multiple_top_level_parens(self, reducer):
        q = "SELECT (a+b), (c+d) FROM t"
        flat = reducer._flatten_subqueries(q)
        assert flat.count("(_sq_)") == 2
        assert flat == "SELECT (_sq_), (_sq_) FROM t"


# ================================
# Extract Base Query Tests
# ================================

class TestExtractBaseQuery:

    def test_no_subquery_wrapper(self, reducer):
        q = "SELECT * FROM books b JOIN authors a ON b.aid = a.id"
        assert reducer._extract_base_query(q) == q

    def test_unwraps_outer_subquery(self, reducer):
        inner = "SELECT * FROM books b JOIN authors a ON b.aid = a.id"
        q = f"SELECT * FROM ({inner}) candidates"
        result = reducer._extract_base_query(q)
        assert "books" in result
        assert "authors" in result
        assert result == inner

    def test_does_not_unwrap_when_join_follows(self, reducer):
        q = "SELECT * FROM (SELECT * FROM foo) t JOIN bar b ON t.id = b.id"
        result = reducer._extract_base_query(q)
        # Should stay at this level because JOIN follows
        assert result == q

    def test_no_from_clause(self, reducer):
        q = "SELECT 1"
        assert reducer._extract_base_query(q) == q


# ================================
# Parse Join Graph Tests
# ================================

class TestParseJoinGraph:

    def test_simple_two_table_join(self, reducer):
        q = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        g = reducer.parse_join_graph(q)
        assert "orders" in g.nodes
        assert "customers" in g.nodes
        assert len(g.edges) == 1
        assert g.aliases.get("orders") == "o"
        assert g.aliases.get("customers") == "c"

    def test_three_table_chain(self, reducer):
        q = """
            SELECT * FROM A a
            JOIN B b ON a.b_id = b.id
            JOIN C c ON b.c_id = c.id
        """
        g = reducer.parse_join_graph(q)
        assert g.nodes == {"A", "B", "C"}
        assert len(g.edges) == 2

    def test_no_alias(self, reducer):
        q = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id"
        g = reducer.parse_join_graph(q)
        assert "orders" in g.nodes
        assert "customers" in g.nodes
        assert len(g.edges) == 1

    def test_inner_join_keyword(self, reducer):
        q = "SELECT * FROM A a INNER JOIN B b ON a.id = b.a_id"
        g = reducer.parse_join_graph(q)
        assert "A" in g.nodes
        assert "B" in g.nodes
        assert len(g.edges) == 1

    def test_keyword_not_treated_as_alias(self, reducer):
        """SQL keywords like WHERE should not be parsed as aliases."""
        q = "SELECT * FROM books WHERE books.id > 5"
        g = reducer.parse_join_graph(q)
        assert "books" in g.nodes
        # "WHERE" should NOT be an alias
        assert "WHERE" not in g.aliases.values()

    def test_single_table_no_join(self, reducer):
        q = "SELECT * FROM books"
        g = reducer.parse_join_graph(q)
        assert g.nodes == {"books"}
        assert len(g.edges) == 0

    def test_join_condition_normalized_to_table_names(self, reducer):
        q = "SELECT * FROM orders o JOIN customers c ON o.cid = c.id"
        g = reducer.parse_join_graph(q)
        t1, t2, cond = g.edges[0]
        assert "orders." in cond
        assert "customers." in cond
        assert "o." not in cond
        assert "c." not in cond

    def test_subquery_wrapper_unwrapped(self, reducer):
        q = """
            SELECT * FROM (
                SELECT * FROM books b JOIN authors a ON b.author_id = a.id
            ) candidates
        """
        g = reducer.parse_join_graph(q)
        assert "books" in g.nodes
        assert "authors" in g.nodes

    def test_as_keyword_alias(self, reducer):
        q = "SELECT * FROM orders AS o JOIN customers AS c ON o.cid = c.id"
        g = reducer.parse_join_graph(q)
        assert g.aliases.get("orders") == "o"
        assert g.aliases.get("customers") == "c"


# ================================
# Alias Resolution Tests
# ================================

class TestAliasToTable:

    def test_resolve_alias(self, reducer):
        g = JoinGraph()
        g.add_node("customers", "c")
        assert reducer._alias_to_table("c", g) == "customers"

    def test_resolve_table_name_directly(self, reducer):
        g = JoinGraph()
        g.add_node("customers")
        assert reducer._alias_to_table("customers", g) == "customers"

    def test_unknown_alias_returns_none(self, reducer):
        g = JoinGraph()
        g.add_node("customers", "c")
        assert reducer._alias_to_table("x", g) is None


# ================================
# Semi-Join Tests
# ================================

class TestSemiJoin:

    def test_basic_semi_join_reduces_left(self, reducer_with_two_tables):
        """orders ⋉ customers: only orders with valid customer_id survive."""
        r = reducer_with_two_tables
        r.semi_join("orders", "customers", "l.customer_id = r.id")
        count = r.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        # Orders 10,11,12 reference customers 1,2 (exist). 13,14 reference 5,6 (don't exist).
        assert count == 3

    def test_semi_join_does_not_add_columns(self, reducer_with_two_tables):
        """Semi-join should keep left table's schema only."""
        r = reducer_with_two_tables
        cols_before = [c[0] for c in r.conn.execute("DESCRIBE orders").fetchall()]
        r.semi_join("orders", "customers", "l.customer_id = r.id")
        cols_after = [c[0] for c in r.conn.execute("DESCRIBE orders").fetchall()]
        assert cols_before == cols_after

    def test_semi_join_reverse_direction(self, reducer_with_two_tables):
        """customers ⋉ orders: only customers with orders survive."""
        r = reducer_with_two_tables
        r.semi_join("customers", "orders", "l.id = r.customer_id")
        count = r.conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        # Only customers 1 and 2 are referenced by orders
        assert count == 2

    def test_semi_join_no_match_empties_table(self, reducer):
        """If nothing matches, table becomes empty."""
        reducer.conn.execute("CREATE TABLE left_t (id INT)")
        reducer.conn.execute("INSERT INTO left_t VALUES (1), (2)")
        reducer.conn.execute("CREATE TABLE right_t (id INT)")
        reducer.conn.execute("INSERT INTO right_t VALUES (99)")
        reducer.semi_join("left_t", "right_t", "l.id = r.id")
        count = reducer.conn.execute("SELECT COUNT(*) FROM left_t").fetchone()[0]
        assert count == 0

    def test_semi_join_all_match(self, reducer):
        """If everything matches, table stays the same size."""
        reducer.conn.execute("CREATE TABLE left_t (id INT)")
        reducer.conn.execute("INSERT INTO left_t VALUES (1), (2)")
        reducer.conn.execute("CREATE TABLE right_t (id INT)")
        reducer.conn.execute("INSERT INTO right_t VALUES (1), (2), (3)")
        reducer.semi_join("left_t", "right_t", "l.id = r.id")
        count = reducer.conn.execute("SELECT COUNT(*) FROM left_t").fetchone()[0]
        assert count == 2

    def test_semi_join_deduplicates(self, reducer):
        """Duplicate rows in left that both match right: DISTINCT keeps one."""
        reducer.conn.execute("CREATE TABLE left_t (id INT, val INT)")
        reducer.conn.execute("INSERT INTO left_t VALUES (1, 10), (1, 10)")
        reducer.conn.execute("CREATE TABLE right_t (id INT)")
        reducer.conn.execute("INSERT INTO right_t VALUES (1)")
        reducer.semi_join("left_t", "right_t", "l.id = r.id")
        count = reducer.conn.execute("SELECT COUNT(*) FROM left_t").fetchone()[0]
        assert count == 1  # DISTINCT


# ================================
# Yannakakis Reduction Tests
# ================================

class TestYannakakisReduction:

    def test_two_table_reduction(self, reducer_with_two_tables):
        """Simple A-B join: rows that don't participate get eliminated."""
        r = reducer_with_two_tables
        g = JoinGraph()
        g.add_node("orders")
        g.add_node("customers")
        g.add_edge("orders", "customers", "orders.customer_id = customers.id")

        reductions = r.yannakakis_reduction(g)

        assert "orders" in reductions
        assert "customers" in reductions

        # Orders: 5 original, 3 survive (customer_id 1,1,2)
        assert reductions["orders"][0] == 5  # original
        assert reductions["orders"][1] == 3  # reduced

        # Customers: 4 original, 2 survive (id 1,2 referenced by orders)
        assert reductions["customers"][0] == 4
        assert reductions["customers"][1] == 2

    def test_three_table_chain_reduction(self, reducer_chain):
        """
        A -> B -> C chain.
        C has ids {1, 2}.
        B: rows 10(c_id=1), 11(c_id=2) survive. 12(c_id=3), 13(c_id=999) don't.
        A: rows 100(b_id=10), 101(b_id=11) survive. Rest don't.
        """
        r = reducer_chain
        g = JoinGraph()
        g.add_node("A")
        g.add_node("B")
        g.add_node("C")
        g.add_edge("A", "B", "A.b_id = B.id")
        g.add_edge("B", "C", "B.c_id = C.id")

        reductions = r.yannakakis_reduction(g)

        assert reductions["C"][1] == 2 # all C rows participate
        assert reductions["B"][1] == 2 # only B rows with valid c_id
        assert reductions["A"][1] == 2 # only A rows with valid b_id

    def test_empty_graph_returns_empty(self, reducer):
        g = JoinGraph()
        assert reducer.yannakakis_reduction(g) == {}

    def test_single_node_no_reduction(self, reducer):
        """Single table: no joins, no reduction possible."""
        reducer.conn.execute("CREATE TABLE solo (id INT)")
        reducer.conn.execute("INSERT INTO solo VALUES (1), (2), (3)")
        reducer.table_sizes = {"solo": 3}

        g = JoinGraph()
        g.add_node("solo")

        reductions = reducer.yannakakis_reduction(g)
        assert reductions["solo"] == (3, 3, 0.0)

    def test_star_schema_reduction(self, reducer_star):
        """
        Star: center joined to leaf1, leaf2, leaf3.
        Only center rows that participate in ALL joins survive.
        """
        r = reducer_star
        g = JoinGraph()
        g.add_node("center")
        g.add_node("leaf1")
        g.add_node("leaf2")
        g.add_node("leaf3")
        g.add_edge("center", "leaf1", "center.id = leaf1.center_id")
        g.add_edge("center", "leaf2", "center.id = leaf2.center_id")
        g.add_edge("center", "leaf3", "center.id = leaf3.center_id")

        reductions = r.yannakakis_reduction(g)

        # leaf1:{1,2}, leaf2:{2,3}, leaf3:{1} => intersection={} -> no center row survives all three, all tables reduced to 0
        for table in ["center", "leaf1", "leaf2", "leaf3"]:
            assert reductions[table][1] == 0, f"{table} should be empty"

    def test_reduction_percentage_calculation(self, reducer_with_two_tables):
        r = reducer_with_two_tables
        g = JoinGraph()
        g.add_node("orders")
        g.add_node("customers")
        g.add_edge("orders", "customers", "orders.customer_id = customers.id")

        reductions = r.yannakakis_reduction(g)

        # orders: 5 -> 3 => 40%
        orig, red, pct = reductions["orders"]
        assert pct == 40.0

        # customers: 4 -> 2 => 50%
        orig, red, pct = reductions["customers"]
        assert pct == 50.0

    def test_full_join_no_reduction(self, reducer):
        """When all rows participate in the join, reduction is 0%."""
        reducer.conn.execute("CREATE TABLE parents (id INT)")
        reducer.conn.execute("INSERT INTO parents VALUES (1), (2)")
        reducer.conn.execute("CREATE TABLE children (id INT, parent_id INT)")
        reducer.conn.execute("INSERT INTO children VALUES (10, 1), (11, 2)")
        reducer.table_sizes = {"parents": 2, "children": 2}

        g = JoinGraph()
        g.add_node("parents")
        g.add_node("children")
        g.add_edge("parents", "children", "parents.id = children.parent_id")

        reductions = reducer.yannakakis_reduction(g)
        assert reductions["parents"][1] == 2
        assert reductions["children"][1] == 2
        assert reductions["parents"][2] == 0.0
        assert reductions["children"][2] == 0.0


# ================================
# Cyclic Graph Folding Tests
# ================================

class TestFoldCyclicGraph:

    def test_acyclic_graph_unchanged(self, reducer):
        """Folding an already-acyclic graph should not change it."""
        g = JoinGraph()
        g.add_node("a")
        g.add_node("b")
        g.add_edge("a", "b", "a.id = b.a_id")
        # 1 edge < 2 nodes => acyclic
        result = reducer.fold_cyclic_graph(g)
        assert result.nodes == {"a", "b"}
        assert len(result.edges) == 1

    def test_triangle_gets_folded(self, reducer):
        """3 nodes, 3 edges (triangle) -> should be folded to acyclic."""
        reducer.conn.execute("CREATE TABLE x (id INT, y_id INT, z_id INT)")
        reducer.conn.execute("INSERT INTO x VALUES (1, 1, 1)")
        reducer.conn.execute("CREATE TABLE y (id INT, z_id INT)")
        reducer.conn.execute("INSERT INTO y VALUES (1, 1)")
        reducer.conn.execute("CREATE TABLE z (id INT)")
        reducer.conn.execute("INSERT INTO z VALUES (1)")

        g = JoinGraph()
        g.add_node("x")
        g.add_node("y")
        g.add_node("z")
        g.add_edge("x", "y", "x.y_id = y.id")
        g.add_edge("y", "z", "y.z_id = z.id")
        g.add_edge("x", "z", "x.z_id = z.id")

        assert g.is_cyclic()
        result = reducer.fold_cyclic_graph(g)
        assert not result.is_cyclic()


# ================================
# Selection Pushdown Tests
# ================================

class TestApplyLocalPredicates:

    def test_single_table_predicate_filters(self, reducer):
        """Predicate on one table should filter that table only."""
        reducer.conn.execute("CREATE TABLE tags (id INT, tag_name VARCHAR)")
        reducer.conn.execute("""
            INSERT INTO tags VALUES
            (1, 'mystery'), (2, 'romance'), (3, 'sci-fi'), (4, 'mystery-thriller')
        """)
        reducer.conn.execute("CREATE TABLE books (id INT, title VARCHAR)")
        reducer.conn.execute("INSERT INTO books VALUES (10, 'Book A'), (11, 'Book B')")

        g = JoinGraph()
        g.add_node("tags", "t")
        g.add_node("books", "b")
        g.add_edge("tags", "books", "tags.id = books.id")

        query = "SELECT * FROM tags t JOIN books b ON t.id = b.id WHERE lower(t.tag_name) LIKE '%mystery%'"
        reducer._apply_local_predicates(query, g)

        count = reducer.conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
        # Only 'mystery' and 'mystery-thriller' match
        assert count == 2

        # books should be untouched
        book_count = reducer.conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
        assert book_count == 2

    def test_no_where_clause_does_nothing(self, reducer):
        reducer.conn.execute("CREATE TABLE t1 (id INT)")
        reducer.conn.execute("INSERT INTO t1 VALUES (1), (2)")

        g = JoinGraph()
        g.add_node("t1")

        query = "SELECT * FROM t1"
        reducer._apply_local_predicates(query, g)

        count = reducer.conn.execute("SELECT COUNT(*) FROM t1").fetchone()[0]
        assert count == 2

    def test_join_condition_not_applied_as_predicate(self, reducer):
        """If WHERE references multiple table aliases, skip (it's a join cond)."""
        reducer.conn.execute("CREATE TABLE aa (id INT, val INT)")
        reducer.conn.execute("INSERT INTO aa VALUES (1, 10), (2, 20)")
        reducer.conn.execute("CREATE TABLE bb (id INT, val INT)")
        reducer.conn.execute("INSERT INTO bb VALUES (1, 10), (2, 20)")

        g = JoinGraph()
        g.add_node("aa", "a")
        g.add_node("bb", "b")
        g.add_edge("aa", "bb", "aa.id = bb.id")

        # This WHERE references both aliases -> should be skipped
        query = "SELECT * FROM aa a JOIN bb b ON a.id = b.id WHERE a.val = b.val"
        reducer._apply_local_predicates(query, g)

        # Both tables untouched
        assert reducer.conn.execute("SELECT COUNT(*) FROM aa").fetchone()[0] == 2
        assert reducer.conn.execute("SELECT COUNT(*) FROM bb").fetchone()[0] == 2

    def test_predicate_on_table_without_alias(self, reducer):
        """Tables referenced without alias (tablename.col) in WHERE."""
        reducer.conn.execute("CREATE TABLE items (id INT, price FLOAT)")
        reducer.conn.execute("""
            INSERT INTO items VALUES (1, 5.0), (2, 15.0), (3, 25.0)
        """)

        g = JoinGraph()
        g.add_node("items")

        query = "SELECT * FROM items WHERE items.price > 10"
        reducer._apply_local_predicates(query, g)

        count = reducer.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        assert count == 2  # price 15 and 25


# ================================
# HAVING-Aware Reduction Tests
# ================================

class TestHavingAwareReduction:

    def test_returns_none_without_having(self, reducer):
        q = "SELECT * FROM books b JOIN authors a ON b.aid = a.id"
        assert reducer.compute_having_aware_reduction(q) is None

    def test_having_count_star(self, reducer):
        """HAVING COUNT(*) >= N filters parent and cascades to child."""
        # Parent: authors with id 1,2,3
        reducer.conn.execute("CREATE TABLE authors (id INT, name VARCHAR)")
        reducer.conn.execute("""
            INSERT INTO authors VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
        """)
        # Child: books - author 1 has 3 books, author 2 has 1, author 3 has 0
        reducer.conn.execute("CREATE TABLE books (id INT, author_id INT)")
        reducer.conn.execute("""
            INSERT INTO books VALUES
            (10, 1), (11, 1), (12, 1),
            (13, 2)
        """)
        reducer.table_sizes = {"authors": 3, "books": 4}

        query = """
            SELECT a.name FROM books b
            JOIN authors a ON b.author_id = a.id
            GROUP BY a.id, a.name
            HAVING COUNT(*) >= 2
        """
        reductions = reducer.compute_having_aware_reduction(query)

        assert reductions is not None
        # Only author 1 has >= 2 books
        assert reductions["authors"][1] == 1  # 1 author survives
        # Only books referencing author 1 survive
        assert reductions["books"][1] == 3

    def test_having_count_distinct(self, reducer):
        """HAVING COUNT(DISTINCT col) >= N should also be matched."""
        reducer.conn.execute("CREATE TABLE parents (id INT)")
        reducer.conn.execute("INSERT INTO parents VALUES (1), (2)")
        reducer.conn.execute("CREATE TABLE children (id INT, parent_id INT)")
        reducer.conn.execute("""
            INSERT INTO children VALUES (10, 1), (11, 1), (12, 1), (13, 2)
        """)
        reducer.table_sizes = {"parents": 2, "children": 4}

        query = """
            SELECT p.id FROM children c
            JOIN parents p ON c.parent_id = p.id
            GROUP BY p.id
            HAVING COUNT(DISTINCT c.id) >= 2
        """
        reductions = reducer.compute_having_aware_reduction(query)
        assert reductions is not None
        # Parent 1 has 3 distinct children >= 2, parent 2 has 1 < 2
        assert reductions["parents"][1] == 1
        assert reductions["children"][1] == 3


# ================================
# Integration / End-to-End Tests
# ================================

class TestIntegration:

    def test_parse_then_reduce_two_tables(self, reducer_with_two_tables):
        """Testing the following sequence: parse query -> build graph -> reduce."""
        r = reducer_with_two_tables
        query = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        g = r.parse_join_graph(query)
        reductions = r.yannakakis_reduction(g)

        assert reductions["orders"][1] == 3
        assert reductions["customers"][1] == 2

    def test_parse_then_reduce_chain(self, reducer_chain):
        """Parse a 3-table chain query and reduce."""
        r = reducer_chain
        query = """
            SELECT * FROM A a
            JOIN B b ON a.b_id = b.id
            JOIN C c ON b.c_id = c.id
        """
        g = r.parse_join_graph(query)
        reductions = r.yannakakis_reduction(g)

        assert reductions["A"][1] == 2
        assert reductions["B"][1] == 2
        assert reductions["C"][1] == 2

    def test_llm_removal_then_parse(self, reducer):
        """LLM calls are removed before parsing the join graph."""
        query = """
            SELECT b.title, llm_complete('{summarize}', '{gpt4}') AS summary
            FROM books b
            JOIN authors a ON b.author_id = a.id
            WHERE llm_filter('{is_good}', '{gpt4}')
        """
        cleaned = reducer.remove_llm_calls(query)
        g = reducer.parse_join_graph(cleaned)

        assert "books" in g.nodes
        assert "authors" in g.nodes
        assert len(g.edges) == 1
        # No LLM residue in the query
        assert "llm_" not in cleaned.lower()

    def test_complete_reduction_with_pushdown(self, reducer):
        """
        Selection pushdown + Yannakakis on a realistic pattern:
        tags filtered by name, then semi-joins cascade to book_tags and books.
        """
        reducer.conn.execute("CREATE TABLE tags (id INT, tag_name VARCHAR)")
        reducer.conn.execute("""
            INSERT INTO tags VALUES
            (1, 'mystery'), (2, 'romance'), (3, 'sci-fi'),
            (4, 'horror'), (5, 'thriller')
        """)
        reducer.conn.execute("CREATE TABLE book_tags (book_id INT, tag_id INT)")
        reducer.conn.execute("""
            INSERT INTO book_tags VALUES
            (100, 1), (101, 2), (102, 3), (103, 4), (104, 5),
            (100, 2), (105, 99)
        """)
        reducer.conn.execute("CREATE TABLE books (id INT, title VARCHAR)")
        reducer.conn.execute("""
            INSERT INTO books VALUES
            (100, 'Dark Night'), (101, 'Love Story'), (102, 'Space Wars'),
            (103, 'Haunted'), (104, 'Chase'), (105, 'Orphan Book'), (106, 'No Tags')
        """)
        reducer.table_sizes = {"tags": 5, "book_tags": 7, "books": 7}

        query = """
            SELECT * FROM books b
            JOIN book_tags bt ON b.id = bt.book_id
            JOIN tags t ON bt.tag_id = t.id
            WHERE lower(t.tag_name) LIKE '%mystery%'
        """
        g = reducer.parse_join_graph(query)
        base_q = reducer._extract_base_query(query)
        reducer._apply_local_predicates(base_q, g)

        # After pushdown: tags should have only 'mystery' (1 row)
        tag_count = reducer.conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
        assert tag_count == 1

        reductions = reducer.yannakakis_reduction(g)

        # tags: 5 -> 1 (only mystery)
        assert reductions["tags"][0] == 5
        assert reductions["tags"][1] == 1

        # book_tags: only (100, 1) references tag_id=1
        assert reductions["book_tags"][1] == 1

        # books: only book 100 referenced
        assert reductions["books"][1] == 1

    def test_word_boundary_in_condition_rewriting(self, reducer):
        """
        Regression: table 'tags' should not match inside 'book_tags'.
        The _rewrite_cond uses \\b word boundary to prevent this.
        """
        reducer.conn.execute("CREATE TABLE tags (id INT)")
        reducer.conn.execute("INSERT INTO tags VALUES (1), (2)")
        reducer.conn.execute("CREATE TABLE book_tags (tag_id INT, book_id INT)")
        reducer.conn.execute("INSERT INTO book_tags VALUES (1, 10), (2, 11), (3, 12)")
        reducer.table_sizes = {"tags": 2, "book_tags": 3}

        g = JoinGraph()
        g.add_node("tags")
        g.add_node("book_tags")
        g.add_edge("tags", "book_tags", "tags.id = book_tags.tag_id")

        reductions = reducer.yannakakis_reduction(g)
        # book_tags row with tag_id=3 has no matching tag -> reduced away
        assert reductions["book_tags"][1] == 2
        assert reductions["tags"][1] == 2

    def test_zero_original_size_no_division_error(self, reducer):
        """Tables with 0 original size should report 0% reduction."""
        reducer.conn.execute("CREATE TABLE empty_t (id INT)")
        reducer.table_sizes = {"empty_t": 0}

        g = JoinGraph()
        g.add_node("empty_t")

        reductions = reducer.yannakakis_reduction(g)
        assert reductions["empty_t"] == (0, 0, 0.0)

    def test_multiple_joins_same_condition_column(self, reducer):
        """Two tables join on the same column name (id) - common pattern."""
        reducer.conn.execute("CREATE TABLE departments (id INT, name VARCHAR)")
        reducer.conn.execute("INSERT INTO departments VALUES (1, 'Eng'), (2, 'Sales'), (3, 'HR')")
        reducer.conn.execute("CREATE TABLE employees (id INT, dept_id INT, name VARCHAR)")
        reducer.conn.execute("""
            INSERT INTO employees VALUES
            (10, 1, 'Alice'), (11, 1, 'Bob'), (12, 2, 'Carol'), (13, 99, 'Dave')
        """)
        reducer.conn.execute("CREATE TABLE projects (id INT, dept_id INT, title VARCHAR)")
        reducer.conn.execute("""
            INSERT INTO projects VALUES
            (100, 1, 'Alpha'), (101, 3, 'Beta'), (102, 88, 'Gamma')
        """)
        reducer.table_sizes = {"departments": 3, "employees": 4, "projects": 3}

        g = JoinGraph()
        g.add_node("departments")
        g.add_node("employees")
        g.add_node("projects")
        g.add_edge("departments", "employees", "departments.id = employees.dept_id")
        g.add_edge("departments", "projects", "departments.id = projects.dept_id")

        reductions = reducer.yannakakis_reduction(g)

        # Only departments with id=1 appear in both employees and projects tables
        assert reductions["departments"][1] == 1
        # Only employees with dept_id=1 (Alice, Bob) survive the semi-joins
        assert reductions["employees"][1] == 2
        # Only projects with dept_id=1 (Alpha) survive the semi-joins
        assert reductions["projects"][1] == 1
