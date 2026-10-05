"""Generates the "Worst-case optimal joins in Hooray" slide deck.

Writes one HTML file per slide plus the deck index (deck.json) in the file
layout of the claude.ai "Slides" artifact type. See README.md for details.

Usage: python3 slides/generate.py [output-dir]   (default: slides/out)
"""

import html
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.join(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "out"), "project")
OUT = os.path.join(BASE, "slides")

INK = "#16202A"
PAPER = "#F6F4EF"
PAPER2 = "#ECE8DF"
BODY = "#3E4A56"
MUTED = "#5F6A74"
ORANGE_TEXT = "#B4501E"
ORANGE = "#D9622B"
BLUE = "#2C64A6"
CARD = "#FBFAF6"
RULE = "#D6CFC2"
LINE = "#9C9486"
CODE_BG = "#1E2A36"
CODE_FG = "#E6E1D6"
CODE_COMMENT = "#9AA7B4"
DARK_FG = "#EDEAE2"
DARK_MUTED = "#B9C2CB"
DARK_ACCENT = "#F0A070"

SERIF = "'Source Serif 4', Georgia, serif"
SANS = "'IBM Plex Sans', Arial, sans-serif"
MONO = "'JetBrains Mono', 'Courier New', monospace"

TITLE = "Worst-case optimal joins in Hooray"

ORDER = ["cover", "datoms", "indexes", "query",
         "binary-plan", "star",
         "var-at-a-time", "result-flat", "result-dfs", "result-bfs",
         "prefix-extender", "gj-old-loop", "extenders", "issues",
         "exec-pattern", "stages", "gj-loop", "gj-planning", "gj-triangle", "or-stages", "resolved",
         "reading", "thanks"]

L_RECAP = "Datomic recap"
L_BINARY = "Binary joins"
L_WCOJ = "Worst-case optimal joins"
L_OLD = "The old interface"
L_NEW = "ExecPattern and stages"
L_END = "Wrap-up"


def num(sid):
    return ORDER.index(sid) + 1


def section(sid, body, notes, footer=True, extra_style=None):
    style = extra_style or (f"background:{PAPER};color:{BODY};font-family:{SANS};"
                            "padding:128px 128px 160px;display:flex;flex-direction:column;gap:48px")
    foot = ""
    if footer:
        foot = (f'<p style="position:absolute;left:128px;bottom:64px;width:1664px;'
                f'font-size:24px;color:{MUTED}">{TITLE} · {num(sid)}</p>\n')
    return (f'<section id="{sid}" data-transition="fade" style="{style}">\n'
            f"{body}\n{foot}<aside>{html.escape(notes)}</aside>\n</section>\n")


def head(label, title):
    return (f'<div style="display:flex;flex-direction:column;gap:12px">\n'
            f'<p style="font-size:24px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:{ORANGE_TEXT}">{label}</p>\n'
            f'<h2 style="font-family:{SERIF};font-size:64px;font-weight:600;line-height:1.1;color:{INK}">{title}</h2>\n'
            f"</div>")


def code_lines(text):
    out = []
    for line in text.strip("\n").split("\n"):
        esc = html.escape(line, quote=False).replace(" ", "&#160;")
        stripped = line.strip()
        if stripped.startswith(";;") or stripped.startswith("//"):
            esc = f'<span style="color:{CODE_COMMENT}">{esc}</span>'
        out.append(esc)
    return "<br>".join(out)


def code(text, width=None, size=24, extra=""):
    w = f"width:{width}px;" if width else ""
    return (f'<div style="{w}background:{CODE_BG};border-radius:16px;padding:40px 48px;{extra}">\n'
            f'<p style="font-family:{MONO};font-size:{size}px;line-height:1.5;color:{CODE_FG};white-space:nowrap">'
            f"{code_lines(text)}</p>\n</div>")


def p(text, size=30, color=BODY, extra=""):
    return f'<p style="font-size:{size}px;line-height:1.4;color:{color};{extra}">{text}</p>'


def b(text):
    return f'<b style="color:{INK}">{text}</b>'


def col(*children, width=None, gap=28, flex=True):
    w = f"width:{width}px;" if width else ("flex:1;" if flex else "")
    return f'<div style="{w}display:flex;flex-direction:column;gap:{gap}px">\n' + "\n".join(children) + "\n</div>"


def row(*children, gap=64, align="start"):
    return f'<div style="display:flex;gap:{gap}px;align-items:{align}">\n' + "\n".join(children) + "\n</div>"


def card(title, text):
    return (f'<div style="flex:1;display:flex;flex-direction:column;gap:12px;background:{CARD};'
            f'border:1px solid {RULE};border-radius:16px;padding:32px 40px">\n'
            f'<h3 style="font-family:{SERIF};font-size:40px;font-weight:600;line-height:1.2;color:{INK}">{title}</h3>\n'
            f"{p(text)}\n</div>")


def step(n, text):
    return ('<div style="display:flex;gap:24px;align-items:start">\n'
            f'<p style="font-family:{SERIF};font-size:40px;font-weight:600;line-height:1.1;color:{ORANGE_TEXT};width:40px">{n}</p>\n'
            f'{p(text, extra="flex:1")}\n</div>')


def table(header, rows, widths, size=30, width=1664):
    def cells(tag, values, first):
        out = []
        for i, v in enumerate(values):
            w = f"width:{widths[i]}%;" if first else ""
            style = f"{w}text-align:left"
            if tag == "th":
                style += f";color:{INK}"
            out.append(f'<{tag} style="{style}">{v}</{tag}>')
        return "".join(out)
    trs = [f'<tr style="background:{PAPER2}">{cells("th", header, True)}</tr>']
    for r in rows:
        trs.append(f"<tr>{cells('td', r, False)}</tr>")
    return (f'<table style="width:{width}px;font-family:{SANS};font-size:{size}px;color:{BODY};padding:14px 20px">\n'
            + "\n".join(trs) + "\n</table>")


# --- tree drawing ------------------------------------------------------------
def tree(nodes, edges, w, h, badges=None, label_mono=False):
    """nodes: name -> (left, center_y, width, label). edges: (parent, child)."""
    lines = []
    for parent, child in edges:
        pl, pcy, pw, _ = nodes[parent]
        cl, ccy, _, _ = nodes[child]
        lines.append(f'<line x1="{pl + pw}" y1="{pcy}" x2="{cl}" y2="{ccy}" stroke="{LINE}" stroke-width="3"/>')
    svg = (f'<svg aria-label="" style="position:absolute;left:0px;top:0px;width:{w}px;height:{h}px" '
           f'width="{w}" height="{h}" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">'
           + "".join(lines) + "</svg>")
    parts = [svg]
    font = MONO if label_mono else SANS
    for name, (left, cy, width, label) in nodes.items():
        badge = ""
        if badges and name in badges:
            badge = (f'<p style="font-family:{MONO};font-size:24px;font-weight:600;line-height:40px;color:{INK};'
                     f'background:{ORANGE};border-radius:20px;width:40px;text-align:center">{badges[name]}</p>')
        style_label = "font-style:italic;" if name == "root" else ""
        parts.append(
            f'<div style="position:absolute;left:{left}px;top:{cy - 28}px;width:{width}px;height:56px;'
            f'display:flex;flex-direction:row;align-items:center;gap:12px;padding:0px 14px;'
            f'background:{CARD};border:2px solid {RULE};border-radius:28px">'
            f'{badge}<p style="font-family:{font};font-size:24px;{style_label}color:{INK};white-space:nowrap">{label}</p></div>')
    return f'<div style="position:relative;width:{w}px;height:{h}px">\n' + "\n".join(parts) + "\n</div>"


COLS = [(0, 96), (156, 200), (436, 180), (696, 230)]
GEO = {
    "root": (0, 260, "root"),
    "Germany": (1, 140, "Germany"), "USA": (1, 380, "USA"),
    "Berlin": (2, 80, "Berlin"), "Munich": (2, 200, "Munich"),
    "NYC": (2, 320, "NYC"), "LA": (2, 440, "LA"),
    "Mitte": (3, 40, "Mitte"), "Kreuzberg": (3, 120, "Kreuzberg"), "Schwabing": (3, 200, "Schwabing"),
    "Manhattan": (3, 280, "Manhattan"), "Brooklyn": (3, 360, "Brooklyn"), "Venice": (3, 440, "Venice"),
}
GEO_NODES = {n: (COLS[c][0], cy, COLS[c][1], label) for n, (c, cy, label) in GEO.items()}
GEO_EDGES = [("root", "Germany"), ("root", "USA"), ("Germany", "Berlin"), ("Germany", "Munich"),
             ("USA", "NYC"), ("USA", "LA"), ("Berlin", "Mitte"), ("Berlin", "Kreuzberg"),
             ("Munich", "Schwabing"), ("NYC", "Manhattan"), ("NYC", "Brooklyn"), ("LA", "Venice")]
DFS = ["Germany", "Berlin", "Mitte", "Kreuzberg", "Munich", "Schwabing",
       "USA", "NYC", "Manhattan", "Brooklyn", "LA", "Venice"]
BFS = ["Germany", "USA", "Berlin", "Munich", "NYC", "LA",
       "Mitte", "Kreuzberg", "Schwabing", "Manhattan", "Brooklyn", "Venice"]


def geo_tree(order=None):
    badges = {n: i + 1 for i, n in enumerate(order)} if order else None
    return tree(GEO_NODES, GEO_EDGES, 926, 480, badges)


slides = {}

# ============================================================================
slides["cover"] = section("cover", "\n".join([
    f'<p style="font-size:24px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:{DARK_ACCENT}">Hooray · an in-memory Datalog engine</p>',
    '<div style="display:flex;flex-direction:column;gap:32px">',
    f'<div style="width:120px;height:8px;background:{ORANGE}"></div>',
    f'<h1 style="font-family:{SERIF};font-size:112px;font-weight:600;line-height:1.05;color:{DARK_FG};width:1400px">Worst-case optimal joins in a Datalog engine</h1>',
    f'<p style="font-size:40px;line-height:1.3;color:{DARK_MUTED}">From binary joins to Generic Join, and the interface that finally held up</p>',
    "</div>",
    '<div style="display:flex;justify-content:space-between;align-items:end">',
    f'<p style="font-size:30px;color:{DARK_FG}">Finn Völkel</p>',
    f'<p style="font-size:30px;color:{DARK_MUTED}">[Event name] · [Date]</p>',
    "</div>",
]), "Hi, I'm Finn. This talk is about worst-case optimal joins in Hooray, a small in-memory Datalog engine with a Datomic-style API that I use as a testbed. Plan: a short recap of Datomic-style databases, how joins are usually done, how worst-case optimal joins build their result, and then the story of Hooray's Generic Join interface: the old PrefixExtender, what went wrong with it, and the ExecPattern and stage design that replaced it.",
    footer=False,
    extra_style=f"background:{INK};color:{DARK_FG};font-family:{SANS};padding:128px;display:flex;flex-direction:column;justify-content:space-between")

# ============================================================================
slides["datoms"] = section("datoms", "\n".join([
    head(L_RECAP, "Data model: every fact is a datom"),
    row(
        col(
            code("""
[:db/add :ada :first-name "Ada"]
""", size=30),
            p(f"{b(':db/add')} asserts a single datom."),
            code("""
{:db/id     :ada
 :name      "Ada"
 :last-name "Lovelace"
 :follows   :alan}
""", size=30),
            p("Transacting an entity map adds one datom per attribute."),
            width=700, flex=False, gap=24),
        col(
            table(["Entity", "Attribute", "Value"], [
                [":ada", ":first-name", '"Ada"'],
                [":ada", ":name", '"Ada"'],
                [":ada", ":last-name", '"Lovelace"'],
                [":ada", ":follows", ":alan"],
            ], [30, 36, 34], width=900),
            p(f"{b('Schema:')} each attribute declares a value type and a cardinality."),
            p(f"Datomic also records the transaction of each datom. Hooray keeps only entity, attribute and value."),
        ),
    ),
]), "A quick recap for anyone who has not used Datomic or one of its relatives like Datascript or XTDB v1. The database is a set of facts, called datoms: entity, attribute, value. The basic operation is :db/add, which asserts one datom. An entity map is just sugar for several datoms with the same entity id. The table shows the datoms from both. Attributes are declared in a schema with a type and a cardinality. Datomic adds the transaction and an added or retracted flag; Hooray only stores the triple.")

# ============================================================================
slides["indexes"] = section("indexes", "\n".join([
    head(L_RECAP, "Covering indexes: the same datoms, sorted four ways"),
    table(["Index", "Sorted by", "Answers"], [
        ["EAV", "entity → attribute → value", "everything about one entity"],
        ["AEV", "attribute → entity → value", "[?e :follows ?f] for a known or every ?e"],
        ["AVE", "attribute → value → entity", '[?e :name "Ada"]: entities by value'],
        ["VAE", "value → attribute → entity", "reverse references: who follows :alan?"],
    ], [12, 40, 48], size=28),
    row(
        code("""
;; AEV: attribute → entity → values
:follows
  :ada   → :alan, :grace
  :alan  → :grace
""", extra="flex:1"),
        code("""
;; AVE: attribute → value → entities
:follows
  :alan  → :ada
  :grace → :ada, :alan
""", extra="flex:1"),
        gap=32),
]), "Every datom lives in several sorted indexes, so each access pattern is a lookup or a range scan. AEV answers: given an attribute and an entity, what are the values. AVE answers: given an attribute and a value, which entities. In Hooray every index is a nested sorted map. Keep that in mind: a nested map is a trie, and tries are exactly what worst-case optimal joins want.")

# ============================================================================
slides["query"] = section("query", "\n".join([
    head(L_RECAP, "Queries: patterns joined by shared variables"),
    row(
        code("""
{:find  [?name ?age]
 :where [[?e :name ?name]
         [?e :age ?age]
         [?e :follows ?f]
         [(< ?age 40)]
         (or [?f :name "Alan"]
             [?f :name "Grace"])
         (not [?e :banned true])]}
""", width=760, size=30),
        col(
            p(f"{b('A pattern [e a v]')} matches datoms: constants filter, variables bind."),
            p(f"{b('A shared variable is a join.')} ?e joins three patterns and the not, ?f joins a pattern and the or."),
            p(f"{b('or')} keeps rows that match any branch. {b('not')} removes rows that match its body."),
            p(f"Each pattern is answered by an index: AVE when the value is known, AEV otherwise."),
        ),
    ),
]), "The query language is Datalog in EDN. A where clause is a list of patterns. Constants restrict, variables bind, and a variable that appears in several patterns joins them. Here: people under 40 who follow Alan or Grace and are not banned. Besides patterns there are predicates like the age check, functions that bind a new variable, or, which keeps a row if any branch matches, and not, which removes rows that match its body. So every query is a multi-way join over the indexes, and the question for the rest of the talk is how to evaluate that join.")

# ============================================================================
plan_nodes = {
    "top": (240, 40, 260, "⋈ on a, c"),
    "mid": (60, 220, 220, "⋈ on b"),
    "T": (420, 220, 200, "T(a,c)"),
    "R": (0, 400, 180, "R(a,b)"),
    "S": (220, 400, 180, "S(b,c)"),
}
plan_edges_svg = []
for parent, child in [("top", "mid"), ("top", "T"), ("mid", "R"), ("mid", "S")]:
    pl, pcy, pw, _ = plan_nodes[parent]
    cl, ccy, cw, _ = plan_nodes[child]
    plan_edges_svg.append(f'<line x1="{pl + pw / 2}" y1="{pcy + 28}" x2="{cl + cw / 2}" y2="{ccy - 28}" stroke="{LINE}" stroke-width="3"/>')
plan_parts = [f'<svg aria-label="" style="position:absolute;left:0px;top:0px;width:640px;height:500px" width="640" height="500" '
              f'viewBox="0 0 640 500" xmlns="http://www.w3.org/2000/svg">' + "".join(plan_edges_svg) + "</svg>"]
for name, (left, cy, width, label) in plan_nodes.items():
    is_join = name in ("top", "mid")
    bg = INK if is_join else CARD
    fg = DARK_FG if is_join else INK
    plan_parts.append(
        f'<div style="position:absolute;left:{left}px;top:{cy - 28}px;width:{width}px;height:56px;display:flex;'
        f'align-items:center;justify-content:center;background:{bg};border:2px solid {INK if is_join else RULE};border-radius:12px">'
        f'<p style="font-family:{MONO};font-size:26px;color:{fg};white-space:nowrap">{label}</p></div>')
plan_parts.append(f'<p style="position:absolute;left:0px;top:452px;width:640px;font-size:24px;color:{ORANGE_TEXT};text-align:center">R ⋈ S builds every 2-path a → b → c</p>')
plan_tree = '<div style="position:relative;width:640px;height:500px">\n' + "\n".join(plan_parts) + "\n</div>"

slides["binary-plan"] = section("binary-plan", "\n".join([
    head(L_BINARY, "The usual way: a tree of binary joins"),
    row(
        col(
            code("""
{:find [?a ?b ?c]
 :where [[?a :follows ?b]   ;; R(a,b)
         [?b :follows ?c]   ;; S(b,c)
         [?a :follows ?c]]} ;; T(a,c)
"""),
            p("Join two relations, materialise the result, then join the next one."),
            p("Each step is a hash or merge join on the variables the two inputs share."),
            p("The optimiser chooses only the order of the pairwise joins."),
            width=960, flex=False),
        plan_tree,
    ),
]), "The triangle query: a follows b, b follows c, and a follows c. A classic relational engine picks a tree of binary joins. Here: first R join S on b, which produces every two-step path, then join that with T on a and c. Each step is a hash join or a sort-merge join. The optimiser can reorder the joins, but the shape stays pairwise and every intermediate result is materialised.")

# ============================================================================
cx, cy = 300, 300
leaves_in = [(240.5, 522.2), (137.4, 462.6), (77.8, 359.5), (77.8, 240.5), (137.4, 137.4), (240.5, 77.8)]
leaves_out = [(359.5, 77.8), (462.6, 137.4), (522.2, 240.5), (522.2, 359.5), (462.6, 462.6), (359.5, 522.2)]
svg_parts = []
for (x, y) in leaves_in:
    svg_parts.append(f'<line x1="{x}" y1="{y}" x2="{cx}" y2="{cy}" stroke="{ORANGE}" stroke-width="6"/>')
for (x, y) in leaves_out:
    svg_parts.append(f'<line x1="{x}" y1="{y}" x2="{cx}" y2="{cy}" stroke="{BLUE}" stroke-width="6"/>')
for (x, y) in leaves_in:
    svg_parts.append(f'<circle cx="{x}" cy="{y}" r="24" fill="{CARD}" stroke="{ORANGE}" stroke-width="5"/>')
for (x, y) in leaves_out:
    svg_parts.append(f'<circle cx="{x}" cy="{y}" r="24" fill="{CARD}" stroke="{BLUE}" stroke-width="5"/>')
svg_parts.append(f'<circle cx="{cx}" cy="{cy}" r="44" fill="{INK}"/>')
star_svg = ('<svg aria-label="A star graph: six accounts follow a hub (orange edges) and the hub follows six other accounts (blue edges)" '
            'width="600" height="600" viewBox="0 0 600 600" xmlns="http://www.w3.org/2000/svg">'
            + "".join(svg_parts) + "</svg>")

slides["star"] = section("star", "\n".join([
    head(L_BINARY, "Skewed data breaks every binary plan"),
    row(
        col(star_svg, width=600, flex=False, gap=0),
        col(
            p(f'A hub: <span style="color:{ORANGE_TEXT}"><b>N/2 accounts follow it</b></span> and '
              f'<span style="color:{BLUE}"><b>it follows N/2 others</b></span>.'),
            row(
                col(f'<p style="font-family:{SERIF};font-size:72px;font-weight:600;line-height:1.1;color:{ORANGE_TEXT}">(N/2)²</p>',
                    p("tuples from any first pairwise join"), gap=8),
                col(f'<p style="font-family:{SERIF};font-size:72px;font-weight:600;line-height:1.1;color:{BLUE}">0</p>',
                    p("triangles in the result"), gap=8),
                gap=32),
            p("All three pairwise joins meet at the hub, so reordering does not help. "
              "Yet no input has more than N¹·⁵ triangles (the AGM bound)."),
            p(f"{b('Worst-case optimal joins')} run within that bound on every input."),
        ),
    ),
]), "Here is why that is a problem. One hub account: N/2 accounts follow it and it follows N/2 others. Joining any two of the three patterns first meets at the hub and builds (N/2) squared tuples, but there is not a single triangle. Atserias, Grohe and Marx showed the output of the triangle query is at most N to the 1.5. Any plan of binary joins is Omega(N squared) on some input. A worst-case optimal join never does more work than that bound, up to log factors.")

# ============================================================================
slides["var-at-a-time"] = section("var-at-a-time", "\n".join([
    head(L_WCOJ, "Join one variable at a time, not one relation"),
    row(
        code("""
for a in R.a ∩ T.a:
  for b in R[a] ∩ S.b:
    for c in S[b] ∩ T[a]:
      emit (a, b, c)
""", width=760, size=30),
        col(
            step("1", "Fix an order of the variables: a, then b, then c."),
            step("2", "At each level, intersect the candidates of every pattern that mentions the variable."),
            step("3", "If each intersection costs about its smallest input, the loop nest stays within the AGM bound."),
            gap=32),
    ),
]), "The idea behind every worst-case optimal join: bind variables one at a time instead of joining relations pairwise. For each variable, every pattern that mentions it offers candidates given what is already bound, and we intersect. The indexes from the recap give each pattern as a trie in whatever order we need. On the star, the innermost intersection is tiny for every a, so the blow-up never happens.")

# ============================================================================
slides["result-flat"] = section("result-flat", "\n".join([
    head(L_WCOJ, "A result set is a trie in disguise"),
    row(
        table(["Country", "City", "District"], [
            ["Germany", "Berlin", "Mitte"],
            ["Germany", "Berlin", "Kreuzberg"],
            ["Germany", "Munich", "Schwabing"],
            ["USA", "NYC", "Manhattan"],
            ["USA", "NYC", "Brooklyn"],
            ["USA", "LA", "Venice"],
        ], [32, 30, 38], size=28, width=640),
        geo_tree(),
        gap=96, align="center"),
    p("Each level is one variable and each root-to-leaf path is one row. Shared prefixes are stored once."),
]), "The flat list of lists on the left is how we usually picture a result. On the right is the same result as a trie, as in factorised databases. If country, city and district are join variables, a variable-at-a-time join builds exactly this trie: level by level, one variable per level. The two classic algorithms differ in the order in which they build it.")

# ============================================================================
slides["result-dfs"] = section("result-dfs", "\n".join([
    head(L_WCOJ, "Leapfrog Triejoin builds the trie depth first"),
    row(
        geo_tree(DFS),
        col(
            p(f"{b('Germany, Berlin, Mitte, Kreuzberg')}, and only then Munich."),
            p("Each level intersects sorted iterators with seek; the next level opens below the current key."),
            p("State is one cursor per pattern and level. A row is emitted when a leaf is reached."),
        ),
        gap=64),
]), "Leapfrog Triejoin, Veldhuizen 2012, walks the result trie depth first. It binds Germany, descends to Berlin, emits Mitte and Kreuzberg, backtracks to Munich, and so on. The numbers show the order nodes are created. Each level is an intersection of sorted iterators that can seek. No intermediate result is materialised, only the current path.")

# ============================================================================
slides["result-bfs"] = section("result-bfs", "\n".join([
    head(L_WCOJ, "Generic Join builds the trie breadth first"),
    row(
        geo_tree(BFS),
        col(
            p(f"{b('All countries, then all cities, then all districts.')} Each level is a list of prefixes."),
            p("Extending (Germany, Berlin) yields (Germany, Berlin, Mitte) and (Germany, Berlin, Kreuzberg)."),
            p("With persistent lists, prefixes share structure, so the trie is stored implicitly."),
        ),
        gap=64),
]), "Generic Join, from Ngo, Re and Rudra, builds the same trie breadth first. It keeps the list of all prefixes of the current level and extends every prefix by one variable. Extending a prefix means adding one level to its subtree. If result tuples are persistent lists, all the extensions of one prefix share it, so the trie is there implicitly. The rest of the talk is about Generic Join in Hooray.")

# ============================================================================
slides["prefix-extender"] = section("prefix-extender", "\n".join([
    head(L_OLD, "The PrefixExtender interface"),
    code("""
typealias Prefix = ResultTuple

interface PrefixExtender {
    fun count(prefix: Prefix): Int
    fun propose(prefix: Prefix): List<Extension>
    fun intersect(prefix: Prefix, extensions: List<Extension>): List<Extension>
}
"""),
    row(
        p(f"{b('count, propose, intersect:')} the three steps of Generic Join, for one prefix.", extra="flex:1"),
        p(f"{b('Prefix:')} the values bound so far, one per variable of the join order.", extra="flex:1"),
    ),
]), "This is the interface Generic Join in Hooray used before the rewrite, modelled on Frank McSherry's description of Generic Join. A prefix is the tuple of values bound so far, one per level. count says how many extensions a pattern would propose for that prefix, propose returns them, and intersect keeps the extensions the pattern agrees with.")

# ============================================================================
slides["gj-old-loop"] = section("gj-old-loop", "\n".join([
    head(L_OLD, "The old join: level by level, prefix by prefix"),
    code("""
// GenericJoin.join(): breadth first over the levels
var prefixes: List<Prefix> = listOf(persistentListOf())
for (extenderSet in extenderSets) {           // extenders of one level
    prefixes = GenericSingleJoin(extenderSet, prefixes).join()
}

// GenericSingleJoin.join(): one prefix at a time
for (prefix in prefixes) {
    val minIndex = extenders.indices.minBy { extenders[it].count(prefix) }
    var extensions = extenders[minIndex].propose(prefix)
    for (i in extenders.indices) if (i != minIndex)
        extensions = extenders[i].intersect(prefix, extensions)
    results.addAll(applyExtensions(prefix, extensions))
}
"""),
]), "The join itself was short. For every level, collect the extenders that participate in it. For every prefix, the extender with the smallest count proposes, all others intersect, and the surviving extensions are appended to the prefix. That is the breadth-first construction from the previous slide, literally. The code is lightly trimmed.")

# ============================================================================
slides["extenders"] = section("extenders", "\n".join([
    head(L_OLD, "Every clause had to be a PrefixExtender"),
    table(["Clause", "count", "propose", "intersect"], [
        ["Triple", "size of the index node", "keys of the node", "lookups in the node"],
        ["and", "smallest child", "smallest child, others intersect", "every child in turn"],
        ["or", "sum of the children", "union of the children", "union of the children"],
        ["Function", "1", "[f(prefix)]", "keep f(prefix) if offered"],
        ["Predicate", "Int.MAX_VALUE", "throws", "filter the extensions"],
        ["not", "Int.MAX_VALUE", "throws", "a nested GenericJoin per prefix"],
    ], [16, 22, 31, 31], size=28),
]), "Every kind of clause was squeezed into the same three methods. Triples walk their nested index along the prefix. and and or combine their children. A function proposes exactly one value. Predicates and not cannot propose anything, so they return Int.MAX_VALUE from count and throw if propose is ever called. not even runs a complete nested Generic Join for every single prefix to decide what to remove.")

# ============================================================================
slides["issues"] = section("issues", "\n".join([
    head(L_OLD, "Where it broke: or leaks branch identity"),
    row(
        code("""
{:find [?name ?age]
 :where [[?e :age ?age]
         (or (and [?e :name "A"]
                  [(< ?age 30)])
             (and [?e :name "B"]
                  [(< ?age 40)]))
         [?e :name ?name]]}
""", width=760),
        code("""
// the or as a PrefixExtender, simplified
fun intersect(prefix, extensions) =
    branches.flatMap { branch ->
        branch.intersect(prefix, extensions)
    }.distinct()
""", extra="flex:1"),
        gap=48),
    p(f"{b('Global variable order:')} ?e → ?age → ?name, the order of first appearance in the query."),
    p(f"{b('The or only ever sees one prefix at one level.')} It unions what its branches accept at that level, "
      "but nothing checks that a single branch accepts the whole tuple."),
]), "This is where the old interface broke. Take an or with two branches, each a triple on ?e and a predicate on ?age. The global variable order is ?e, then ?age, then ?name, the order in which the variables first appear. As a PrefixExtender, the or is asked about one prefix and one level at a time, and the natural implementation unions whatever its branches accept at that level. Each branch is itself spread over several levels: the triple takes part at level ?e, the predicate at level ?age. So the or never asks whether one branch accepts the complete tuple. With entities a and b, both 35: at level ?e branch A admits a and branch B admits b. At level ?age for the prefix a, branch B's predicate accepts 35, and branch B's triple is never asked again whether a has name B. So the query returns A, 35 as well as B, 35. A per-branch trie patched this case, but only partially. This is the point where I decided to rewrite the engine, following Frank McSherry's datatoad.")

# ============================================================================
slides["exec-pattern"] = section("exec-pattern", "\n".join([
    head(L_NEW, "The new ExecPattern interface"),
    row(
        code("""
interface ExecPattern {
    val variables: Set<Variable>
    // lower a row's proposal if cheaper
    fun count(input: BindingSet,
              added: List<Variable>,
              proposals: List<Proposal>)
        : List<Proposal>

    // extend if added is non-empty, else filter
    fun join(input: BindingSet,
             added: List<Variable>,
             targetVariables: List<Variable>)
        : BindingSet
}
""", width=880),
        col(
            p(f"{b('Variables, not levels.')} A pattern names the variables it binds."),
            p(f"{b('A BindingSet per call.')} All rows of a stage at once, not one prefix."),
            p(f"{b('added holds one or more variables')}, or none: then join only filters."),
            p(f"{b('Planning is separate.')} In Clojure, each pattern says what it can ground given what is bound."),
        ),
    ),
]), "The new interface, inspired by ExecAtom in datatoad. Two methods instead of three. count updates the per-row proposal if this pattern is cheaper. join either extends the input by the added variables, or, when added is empty, filters it. Everything is addressed by variable name, and every call gets a whole binding set. Planning moved to Clojure: each pattern has a groundable function that says which variables it can introduce given the bound ones.")

# ============================================================================
slides["stages"] = section("stages", "\n".join([
    head(L_NEW, "Stages: the plan as a sequence of layout changes"),
    row(
        col(
            p(f"{b('BindingSet:')} ordered variables and rows of the same arity."),
            p(f"{b('Stage:')} the variables it adds, who may propose them, who participates, and the output layout."),
            code("""
{:added            [?name]
 :proposers        [1]
 :participants     [1 2]
 :target-variables [?e ?name]}
"""),
            width=700, flex=False),
        col(
            step("1", "Every proposer counts its candidates for every row."),
            step("2", "Per row, the cheapest positive count wins. Rows nobody can extend are dropped."),
            step("3", "Rows are grouped by winner, and the winner extends its group."),
            step("4", "Every other participant validates. The groups are unioned."),
            p(f"A stage that adds nothing is {b('validation only')}: every participant filters."),
            gap=24),
    ),
]), "A plan is a list of stages. Each stage takes a binding set to a new layout: it adds some variables, and lists the participating patterns. Some participants may propose, the rest validate. Executing a proposing stage is count, pick the cheapest proposer per row, shard the rows by proposer, extend, and validate with everyone else. A stage that adds no variables just filters, which is where predicates, not and complete or clauses live.")

# ============================================================================
slides["gj-loop"] = section("gj-loop", "\n".join([
    head(L_NEW, "Executing a stage: proposer chosen per row"),
    code("""
// every proposer may lower each row's (proposer, count)
val proposals = proposers.fold(initial) { acc, p ->
    p.count(input, stage.added, acc)
}

// group rows by their winning proposer, extend, then validate
for ((id, rows) in shards) {
    val proposed = patterns[id].join(input.selectRows(rows),
                                     stage.added, stage.targetVariables)
    result = result.union(validateAll(proposed, others(id)))
}
"""),
    row(
        p(f"{b('Still breadth first.')} A stage handles every row before the next stage starts.", extra="flex:1"),
        p(f"{b('Still worst-case optimal.')} The smallest candidate set drives every row.", extra="flex:1"),
    ),
]), "The core of GenericJoinEngine, slightly simplified. It is the same Generic Join as before, count, propose, intersect, but over a whole binding set, with the choice of proposer made row by row. Skew in one row does not affect the choice for others.")

# ============================================================================
slides["gj-planning"] = section("gj-planning", "\n".join([
    head(L_NEW, "Planning: who may propose, who validates"),
    table(["Pattern", "Proposes", "Validates"], [
        ["Triple", "either variable, via AEV or AVE", "when its variables are bound"],
        ["Predicate", "nothing", "once all arguments are bound"],
        ["Function", "its output, after its arguments", "a bound output against the result"],
        ["or", "all missing variables, alone", "semijoin once all are bound"],
        ["not", "nothing", "antijoin once all are bound"],
    ], [20, 40, 40], size=28),
    p("The planner takes the next variable that some pattern can ground, preferring the order of appearance."),
]), "Each pattern kind has its own rule for when it may propose and when it validates. Triples can propose either side. Functions propose their output only after their arguments are bound, and otherwise compare. or proposes all its missing variables at once and alone in its stage. Predicates and not never propose. The planner only picks a variable that some pattern can actually ground.")

# ============================================================================
slides["gj-triangle"] = section("gj-triangle", "\n".join([
    head(L_NEW, "The triangle query as three stages"),
    f'<p style="font-family:{MONO};font-size:24px;line-height:1.5;color:{INK}">P1 = [?a :follows ?b]&#160;&#160;&#160;P2 = [?b :follows ?c]&#160;&#160;&#160;P3 = [?a :follows ?c]</p>',
    table(["Stage", "Adds", "Candidates per row", "Result"], [
        ["1", "?a", "P1, P3: every entity with :follows", "a tie; P1 proposes"],
        ["2", "?b", "P1: follows(a) · P2: every entity with :follows", "usually P1 proposes"],
        ["3", "?c", "P2: follows(b) · P3: follows(a)", "the smaller side proposes"],
    ], [12, 10, 50, 28], size=28),
    p(f"{b('On the star:')} rows with a = hub die in stage 2, because followees follow nobody. "
      "For each follower, stage 3 proposes from follows(a), which has one entry. The work stays linear."),
]), "Back to the triangle, with variable order a, b, c. In every stage the losing proposer validates. Stage 3 is exactly the intersection of follows(a) and follows(b). On the star, every row is cut off early, because the smaller side always drives.")

# ============================================================================
slides["or-stages"] = section("or-stages", "\n".join([
    head(L_NEW, "or and not run as nested plans"),
    row(
        code("""
{:find [?name ?age]
 :where [[?e :age ?age]
         (or (and [?e :name "A"]
                  [(< ?age 30)])
             (and [?e :name "B"]
                  [(< ?age 40)]))
         [?e :name ?name]]}
;; => [["B" 35]]
""", width=760),
        col(
            step("1", "The triples bind ?e and ?age. The or cannot propose: no branch can ground ?age."),
            step("2", "Once ?e and ?age are bound, each branch runs as its own plan over those rows."),
            step("3", "Branch results are unioned and matched back. Branch A keeps nothing, branch B keeps (\"b\", 35)."),
            step("4", "not works the same way, as one antijoin over the whole BindingSet."),
            gap=24),
    ),
]), "How the or query from before runs now. The or cannot introduce age, because branch two only has a predicate on it, so the triples bind e and age first. Once both are bound, the or validates as a unit: each branch is a complete nested plan that receives the bound rows as an input relation. Branch identity cannot leak, because a branch only ever sees whole tuples. not is the same idea with an antijoin. I checked: the query now returns only B, 35.")

# ============================================================================
slides["resolved"] = section("resolved", "\n".join([
    head(L_NEW, "What Stages and ExecPattern fixed"),
    table(["Problem", "PrefixExtender", "Stages + ExecPattern"], [
        ["Variable order", "one global order, fixed up front", "planner picks a groundable variable"],
        ["Functions", "output level after the inputs", "propose after inputs, else validate"],
        ["or branches", "unioned level by level", "each branch is a nested plan"],
        ["Several variables", "one per level", "a stage may add several"],
        ["not, predicates", "fake count, throw in propose", "validation-only participants"],
        ["Granularity", "one prefix per call", "a whole BindingSet per call"],
    ], [22, 39, 39], size=28),
    p("Both failing queries now return the right answer, and so does a query whose functions need b before a and a before b."),
]), "Summary of the rewrite. The planner chooses variables that can actually be grounded, so clause order no longer matters for functions. Functions whose output is already bound simply validate, which handles mutually dependent functions. or and not run as nested plans. A stage can add several variables. Patterns that cannot propose are not forced to pretend. And everything is batched over binding sets. I ran the examples from this talk against the current engine: the or query returns B 35, the inc age query returns y, and [(inc a) b], [(dec b) a], [a :foo b] returns [1 2].")

# ============================================================================
def ref(text):
    return p(text, size=28)


slides["reading"] = section("reading", "\n".join([
    head(L_END, "Further reading"),
    row(
        col(
            f'<h3 style="font-family:{SERIF};font-size:40px;font-weight:600;line-height:1.2;color:{INK}">Papers</h3>',
            ref("Atserias, Grohe, Marx. <i>Size bounds and query plans for relational joins.</i> FOCS 2008."),
            ref("Veldhuizen. <i>Leapfrog Triejoin: a simple, worst-case optimal join algorithm.</i> ICDT 2014."),
            ref("Ngo, Ré, Rudra. <i>Skew strikes back: new developments in the theory of join algorithms.</i> SIGMOD Record 2013."),
            gap=24),
        col(
            f'<h3 style="font-family:{SERIF};font-size:40px;font-weight:600;line-height:1.2;color:{INK}">Code and posts</h3>',
            p('<a href="https://github.com/FiV0/hooray2">github.com/FiV0/hooray2</a>', size=24),
            p('<a href="https://github.com/frankmcsherry/datatoad">github.com/frankmcsherry/datatoad</a>', size=24),
            p('<a href="https://finnvolkel.com/wcoj-generic-join">finnvolkel.com/wcoj-generic-join</a>', size=24),
            p('<a href="https://finnvolkel.com/wcoj-wcoj-meets-dbsp">finnvolkel.com/wcoj-wcoj-meets-dbsp</a>', size=24),
            width=620, flex=False, gap=24),
    ),
]), "The papers behind the talk, the Hooray repository, datatoad which inspired the new interface, and my blog posts on Generic Join and on result representation.")

# ============================================================================
slides["thanks"] = section("thanks", "\n".join([
    f'<div style="width:120px;height:8px;background:{ORANGE}"></div>',
    f'<h1 style="font-family:{SERIF};font-size:112px;font-weight:600;line-height:1.05;color:{DARK_FG}">Questions?</h1>',
    '<div style="display:flex;flex-direction:column;gap:12px">',
    f'<p style="font-size:40px;line-height:1.3;color:{DARK_FG}">github.com/FiV0/hooray2</p>',
    f'<p style="font-size:40px;line-height:1.3;color:{DARK_MUTED}">finnvolkel.com</p>',
    "</div>",
]), "Thanks. Happy to take questions.", footer=False,
    extra_style=f"background:{INK};color:{DARK_FG};font-family:{SANS};padding:128px;display:flex;flex-direction:column;justify-content:center;gap:48px")

assert list(slides) == ORDER, set(ORDER) ^ set(slides)

os.makedirs(OUT, exist_ok=True)
for sid, content in slides.items():
    with open(os.path.join(OUT, f"{sid}.html"), "w") as f:
        f.write(content)

# Remove slides that are no longer part of the deck.
removed = []
for name in sorted(os.listdir(OUT)):
    sid, ext = os.path.splitext(name)
    if ext == ".html" and sid not in slides:
        os.remove(os.path.join(OUT, name))
        removed.append(sid)

deck = {
    "v": 4,
    # Kept fixed so that republishing does not change the deck's creation record.
    "createdOnFiles": {"v": 1, "at": "2026-10-05T12:00:00Z"},
    "lists": "css",
    "title": "Worst-Case Optimal Joins in Hooray",
    "cover": "cover",
    "order": ORDER,
    "sections": {
        "s1": {"description": "A short recap of Datomic-style databases: datoms, covering indexes and the query language.", "start": "cover"},
        "s2": {"description": "How joins are usually done: a tree of binary joins, and how it blows up on the triangle query.", "start": "binary-plan"},
        "s3": {"description": "Worst-case optimal joins and the result set as a trie, built depth first or breadth first.", "start": "var-at-a-time"},
        "s4": {"description": "Hooray's old PrefixExtender interface for Generic Join and where it broke down.", "start": "prefix-extender"},
        "s5": {"description": "The ExecPattern interface and stages, and how they fix the old problems.", "start": "exec-pattern"},
        "s6": {"description": "Further reading and questions.", "start": "reading"},
    },
    "faces": {
        "source-serif-4": {"family": "Source Serif 4", "href": "https://fonts.googleapis.com/css2?family=Source+Serif+4:wght@400..700&display=swap"},
        "ibm-plex-sans": {"family": "IBM Plex Sans", "href": "https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&display=swap"},
        "jetbrains-mono": {"family": "JetBrains Mono", "href": "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&display=swap"},
    },
    "designSystems": [],
}
with open(os.path.join(BASE, "deck.json"), "w") as f:
    json.dump(deck, f, indent=2, ensure_ascii=False)
    f.write("\n")

print(f"Wrote {len(slides)} slides and deck.json to {BASE}")
if removed:
    print("Removed stale slides:", ", ".join(removed))
